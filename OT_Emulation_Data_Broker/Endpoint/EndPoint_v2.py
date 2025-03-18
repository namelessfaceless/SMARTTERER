# Copyright 2021 National Technology & Engineering Solutions of Sandia, LLC (NTESS). 
# Under the terms of Contract DE-NA0003525 with NTESS, the U.S. Government retains 
# certain rights in this software.
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
import threading
import logging
import queue
import time
import socket
import sys
import os
import zmq
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from pymodbus.payload import BinaryPayloadBuilder, BinaryPayloadDecoder
from pymodbus.constants import Endian

class Data_Repo(object):
    """
    Data_Repo manages a repository of values indexed by tag names.

    Inputs:
        SetString (str): A comma-separated string containing alternating tag names and memory values.
                         Example: "Tag1,1,Tag2,2,Tag3,3"
    
    Outputs / Attributes:
        self.Tags: List of all tag names as extracted from SetString.
        self.Mem: List of integers corresponding to the memory values.
        self.Tag_NoDuplicates: List of unique tag names.
        self.Store: Dictionary mapping each unique tag to a default value of 0.0.
    
    Methods:
        write(Tag, Value): Sets the value for a given tag in the repository.
        read(Tag): Retrieves the stored value for a given tag.
        UDP_TAGS(): Returns a list of unique tags.
    """
    def __init__(self, SetString):
        # Split the input string by commas.
        Strings = SetString.split(",")
        # Extract tag names (every other element starting at index 0).
        self.Tags = Strings[::2]
        # Extract memory strings (every other element starting at index 1).
        Mem_Strings = Strings[1::2]
        # Convert memory strings to integers.
        self.Mem = [int(n) for n in Mem_Strings]
        # Create a list of unique tags.
        self.Tag_NoDuplicates = list(set(self.Tags))
        # Initialize the store dictionary with each unique tag set to 0.0.
        Val = [0.0] * len(self.Tag_NoDuplicates)
        self.Store = dict(zip(self.Tag_NoDuplicates, Val))

    def write(self, Tag, Value):
        """
        Update the stored value for a given tag.

        Inputs:
            Tag (str): The tag identifier.
            Value (numeric): The value to store.
        
        Outputs:
            None; updates self.Store.
        """
        self.Store[Tag] = Value

    def read(self, Tag):
        """
        Retrieve the stored value for a given tag.

        Inputs:
            Tag (str): The tag identifier.
        
        Returns:
            The stored value corresponding to the tag.
        """
        return self.Store[Tag]

    def UDP_TAGS(self):
        """
        Get the list of unique tags.

        Returns:
            List of unique tag names.
        """
        return self.Tag_NoDuplicates


# Define class for interfacing with Modbus PLCs.
class MB_PLC:
    """
    MB_PLC handles communication with a PLC using Modbus protocol.

    Inputs:
        IP (str): The IP address of the PLC.
        Port (int): The port number for the Modbus connection.
    
    Attributes:
        ip: The PLC IP address.
        port: The PLC port.
        client: The Modbus client instance.
        byteOrder: Byte order setting (default BIG).
        wordOrder: Word order setting (default BIG).
        mlock: A threading.Lock instance to ensure thread-safe communication.
        Mem_default: Default memory format for reading/writing if none is specified.
    
    Methods:
        connect(): Connects to the PLC.
        read(mem_addr, formating=None): Reads data from PLC registers.
        readcoil(mem_addr): Reads a single coil (boolean) from the PLC.
        writecoil(mem_addr, value): Writes a boolean value to a coil.
        write(mem_addr, value, formating=None): Writes a value to PLC registers.
        close(): Closes the PLC connection.
        __repr__(): Returns a string representation of the PLC.
    """
    def __init__(self, IP, Port):
        self.ip = IP
        self.Mem_default = '32_float'  # Default memory format
        self.port = Port
        self.client = ModbusClient(IP, port=self.port)
        self.byteOrder = Endian.BIG
        self.wordOrder = Endian.BIG
        self.mlock = threading.Lock()

    def connect(self):
        """
        Connect to the PLC using the Modbus client.

        Inputs:
            None
        
        Outputs:
            Establishes a connection to the PLC.
        """
        client = self.client
        client.connect()

    def read(self, mem_addr, formating=None):
        """
        Read a value from the PLC registers.

        Inputs:
            mem_addr (int): The memory address/register to read.
            formating (str, optional): Format specifier (e.g., "32_float"). Defaults to self.Mem_default.
        
        Returns:
            Decoded value read from the PLC, or None if reading/decoding fails.
        """
        # Define local helper functions for various decoding options.
        def float_64(decode):
            return decode.decode_64bit_float()
        def float_32(decode):
            return decode.decode_32bit_float()
        def float_16(decode):
            return decode.decode_16bit_float()
        def int_64(decode):
            return decode.decode_64bit_int()
        def int_32(decode):
            return decode.decode_32bit_int()
        def int_16(decode):
            return decode.decode_16bit_int()
        def uint_64(decode):
            return decode.decode_64bit_uint()
        def uint_32(decode):
            return decode.decode_32bit_uint()
        def uint_16(decode):
            return decode.decode_16bit_uint()

        if formating is None:
            formating = self.Mem_default
        Format = formating.split('_')

        # Determine number of registers to read based on bit width.
        if int(Format[0]) >= 16:
            count = int(int(Format[0]) / 16)
        else:
            count = 1

        client = self.client

        try:
            # Read registers from the PLC.
            results = client.read_holding_registers(mem_addr, count, unit=1)
        except:
            results = None

        # Mapping of formatting to corresponding decoding functions.
        Decode_dict = {
            '16_float': float_16,
            '32_float': float_32,
            '64_float': float_64,
            '16_int': int_16,
            '32_int': int_32,
            '64_int': int_64,
            '16_uint': uint_16,
            '32_uint': uint_32,
            '64_uint': uint_64
        }

        try:
            # Decode the payload using the specified byte and word order.
            decoder = BinaryPayloadDecoder.fromRegisters(results.registers,
                                                         byteorder=self.byteOrder,
                                                         wordorder=self.wordOrder)
            return Decode_dict[formating](decoder)
        except:
            return None

    def readcoil(self, mem_addr):
        """
        Read a single coil (boolean value) from the PLC.

        Inputs:
            mem_addr (int): Memory address for the coil.
        
        Returns:
            Boolean value read from the coil.
        """
        client = self.client
        self.mlock.acquire()
        result = client.read_coils(mem_addr, 1)
        self.mlock.release()
        return result.bits[0]

    def writecoil(self, mem_addr, value):
        """
        Write a boolean value to a coil in the PLC.

        Inputs:
            mem_addr (int): Memory address of the coil.
            value (bool): The value to write.
        
        Outputs:
            Writes the coil value; no return.
        """
        client = self.client
        self.mlock.acquire()
        client.write_coil(mem_addr, value)
        self.mlock.release()

    def write(self, mem_addr, value, formating=None):
        """
        Write a value to the PLC registers.

        Inputs:
            mem_addr (int): Memory address/register to write to.
            value (numeric): Value to be written.
            formating (str, optional): Format specifier (e.g., "32_float"). Defaults to self.Mem_default.
        
        Outputs:
            Sends the write payload to the PLC; prints error messages if unsuccessful.
        """
        # Define local helper functions for various encoding options.
        def float_64(build, value):
            build.add_64bit_float(float(value))
        def float_32(build, value):
            build.add_32bit_float(float(value))
        def float_16(build, value):
            build.add_16bit_float(float(value))
        def int_16(build, value):
            build.add_16bit_int(value)
        def int_32(build, value):
            build.add_32bit_int(value)
        def int_64(build, value):
            build.add_64bit_int(value)
        def uint_16(build, value):
            build.add_16bit_uint(value)
        def uint_32(build, value):
            build.add_32bit_uint(value)
        def uint_64(build, value):
            build.add_64bit_uint(value)

        if formating is None:
            formating = self.Mem_default
        Format = formating.split('_')

        # For integer formats, ensure that value is an integer.
        if Format[1] == 'int' or Format[1] == 'uint':
            if type(value) is not int:
                value = int(value)

        # Determine how many registers to write based on bit width.
        if int(Format[0]) >= 16:
            count = int(Format[0]) / 16
        else:
            count = 1

        client = self.client

        # Start building the payload.
        builder = BinaryPayloadBuilder(byteorder=self.byteOrder, wordorder=self.wordOrder)

        # Mapping for encoding functions.
        Encode_dict = {
            '16_float': float_16,
            '32_float': float_32,
            '64_float': float_64,
            '16_int': int_16,
            '32_int': int_32,
            '64_int': int_64,
            '16_uint': uint_16,
            '32_uint': uint_32,
            '64_uint': uint_64
        }

        # Encode the provided value.
        Encode_dict[formating](builder, value)
        payload = builder.to_registers()

        # Attempt to write the payload to the PLC.
        try:
            Check_write = client.write_registers(mem_addr, payload)
        except:
            Check_write = None
            print('First write failed - IP:%s\n' % self.ip)
            pass
        
        if Check_write is not None:
            while Check_write.isError():
                try:
                    Check_write = client.write_registers(mem_addr, payload)
                except:
                    pass
        else:
            print("Client Not Connected!")

    def close(self):
        """
        Close the connection to the PLC.

        Inputs:
            None
        
        Outputs:
            Closes the Modbus client connection.
        """
        client = self.client
        client.close()

    def __repr__(self):
        """
        Return a string representation of the MB_PLC instance.

        Returns:
            A string representing the PLC with its IP.
        """
        return "MB_PLC('{}')".format(self.ip)


def initialization():
    """
    Initialize system configuration by receiving and parsing a configuration string via ZMQ.

    Inputs:
        None directly (receives a configuration message via a bound ZMQ REP socket on port 6666).
    
    Returns:
        A tuple containing:
          - IP_PLCs (list of str): List of PLC IP addresses.
          - nPLC (int): Number of PLCs.
          - Sensor_Loc (list or int): Sensor location configuration.
          - Actuator_Loc (list or int): Actuator location configuration.
          - Byte_order (list of str): Byte order settings for each PLC.
          - Word_order (list of str): Word order settings for each PLC.
          - Mem_Format (list of str): Memory format settings for each PLC.
          - Port (list of int): Port numbers for each PLC.
          - Time_Mem (list of int): Memory location for time stamps for each PLC.
          - scanTime (list of float): Scan times for each PLC.
          - Sensor_Tags (list of str): Sensor tag names.
          - Sensor_Mem (list or int): Sensor memory addresses.
          - Actuator_Tags (list of str): Actuator tag names.
          - Actuator_Mem (list or int): Actuator memory addresses.
          - data (list): Remaining data from the configuration message.
    
    Notes:
        The function expects a very specific format in the incoming message. Clarification might be needed.
    """
    context = zmq.Context()
    reciever = context.socket(zmq.REP)
    reciever.bind("tcp://*:6666")
    
    msgFromServer = reciever.recv()
    msg = str(msgFromServer, 'UTF-8')
    data = msg.split(":")
    reply = bytes(str(data[0]) + " has initialized", 'utf-8')
    reciever.send(reply)
    reciever.close()
    
    IP_PLCs = data[1].split(",")
    nPLC = len(IP_PLCs)
    if nPLC > 1 and data[11] == "NULL":
        logging.info("Config Error!\nMultiple PLCs with no MultiPLC JSON config!!")
        sys.exit(0)
    
    Endian_Key = data[9].split(",")
    Format_Key = data[8].split(",")
    sensor_name_key = data[3].split(",")
    actuator_name_key = data[5].split(",")

    MultiPLC = data[11].split(";")
    if MultiPLC[0] != "NULL":
        for val in MultiPLC:
            key = val.split("=")
            Loc = key[1].split(",")
            if key[0].lower() == "s":
                Sensor_Loc = [int(n) for n in Loc]
            if key[0].lower() == "a":
                Actuator_Loc = [int(n) for n in Loc]
    else:
        if sensor_name_key == "NULL":
            Sensor_Loc = 0
        else:
            Sensor_Loc = int(len(sensor_name_key) / 2)
        if actuator_name_key == "NULL":
            Actuator_Loc = 0
        else:
            Actuator_Loc = int(len(actuator_name_key) / 2)
    
    if (len(Endian_Key) / 2) <= nPLC and len(Endian_Key) >= 2:
        Byte_order = Endian_Key[::2]
        Word_order = Endian_Key[1::2]
        Byte_order.extend([Byte_order[-1] for i in range(nPLC - len(Byte_order))])
        Word_order.extend([Word_order[-1] for i in range(nPLC - len(Word_order))])
    elif len(Endian_Key) % 2:
        logging.info("Config Error!\nEndianness setting must be a pair! ByteOrder,WordOrder (Big,Big) ")
    else:
        Byte_order = Endian_Key[::2]
        Word_order = Endian_Key[1::2]

    if len(Format_Key) <= nPLC:
        Mem_Format = Format_Key
        Mem_Format.extend([Format_Key[-1] for i in range(nPLC - len(Format_Key))])
    else:
        Mem_Format = Format_Key

    Port_Key = data[10].split(",")
    if len(Port_Key) <= nPLC:
        Port = [int(n) for n in Port_Key]
        Port.extend([Port[-1] for i in range(nPLC - len(Port_Key))])
    else:
        Port = [int(n) for n in Port_Key]
    
    Time_Mem_Key = data[7].split(",")
    if len(Time_Mem_Key) <= nPLC:
        Time_Mem = [int(n) for n in Time_Mem_Key]
        Time_Mem.extend([-1 for i in range(nPLC - len(Time_Mem))])
    else:
        Time_Mem = [int(n) for n in Time_Mem_Key]

    scanTime_Key = data[6].split(",")
    if len(Time_Mem_Key) <= nPLC:
        scanTime = [float(n) for n in scanTime_Key]
        scanTime.extend([0 for i in range(nPLC - len(scanTime))])
    else:
        scanTime = [float(n) for n in scanTime_Key]

    if sensor_name_key[0] != "NULL" and len(sensor_name_key) > 1:
        Sensor_Tags = sensor_name_key[::2]
        Sensor_Mem = sensor_name_key[1::2]
    else:
        Sensor_Tags = None
        Sensor_Mem = -1
    if actuator_name_key[0] != "NULL" and len(actuator_name_key) > 1:
        Actuator_Tags = actuator_name_key[::2]
        Actuator_Mem = actuator_name_key[1::2]
    else:
        Actuator_Tags = None
        Actuator_Mem = -1

    data = data[1:len(data)-1]
    return (IP_PLCs, nPLC, Sensor_Loc, Actuator_Loc, Byte_order, Word_order,
            Mem_Format, Port, Time_Mem, scanTime, Sensor_Tags, Sensor_Mem,
            Actuator_Tags, Actuator_Mem, data)


class Connector:
    """
    Connector manages the communication between a PLC, a Data_Repo, and the server for actuator signals.

    Inputs:
        PLC: An instance of MB_PLC for communicating with the PLC.
        serAdd: A queue that contains the server address for actuator communication.
        Data: An instance of Data_Repo holding sensor/actuator values.
        Lock: A threading.Lock instance for synchronizing access to Data.
        Event: A threading.Event used to signal termination of the communication thread.
    
    Attributes:
        Time_Mem (int): Memory location for time stamp data.
        Scan_Time (float): Delay time between scans.
        actuator (bool): Flag to indicate if actuator communication is enabled.
        sensor (bool): Flag to indicate if sensor communication is enabled.
        SensorTags (list): List of sensor tag names.
        SensorMem (list): List of sensor memory addresses.
        ActuatorTags (list): List of actuator tag names.
        ActuatorMem (list): List of actuator memory addresses.
        Actuator_String (str): String used to configure actuator tags/memory.
        Sensor_String (str): String used to configure sensor tags/memory.
        thread: Thread instance that runs the Agent function.
    
    Methods:
        Set(**kwargs): Configures connector settings using keyword arguments.
        Agent(): The main function running in a separate thread for PLC communication.
        run(): Starts the Agent in a new thread.
        stop(): Signals the Agent thread to stop and waits for its termination.
        wait(): Blocks until the Agent thread finishes.
        __repr__(): Returns a string representation of the Connector instance.
    """
    def __init__(self, PLC, serAdd, Data, Lock, Event):
        self.PLC = PLC
        self.serAdd = serAdd
        self.Data = Data
        self.Lock = Lock
        self.Event = Event
        self.Time_Mem = -1
        self.Scan_Time = 0.1
        self.actuator = False
        self.sensor = False
        self.SensorTags = []
        self.SensorMem = []
        self.ActuatorTags = []
        self.ActuatorMem = []
        self.Actuator_String = ''
        self.Sensor_String = ''
        self.thread = None

    def Set(self, **kwargs):
        """
        Configure the connector settings.

        Keyword Arguments (kwargs):
            Time_Mem (int): Memory location for time stamp.
            Scan_Time (float): Delay between communication cycles.
            actuator (bool): Enable/disable actuator communication.
            sensor (bool): Enable/disable sensor communication.
            SensorTags (list): List of sensor tag names.
            SensorMem (list): List of sensor memory addresses.
            ActuatorTags (list): List of actuator tag names.
            ActuatorMem (list): List of actuator memory addresses.
            Actuator_String (str): Comma-separated string for actuator configuration.
            Sensor_String (str): Comma-separated string for sensor configuration.
        
        Outputs:
            Updates the instance attributes based on kwargs.
        """
        options = {
            'Time_Mem': self.Time_Mem,
            'Scan_Time': self.Scan_Time,
            'actuator': self.actuator,
            'sensor': self.sensor,
            'SensorTags': self.SensorTags,
            'SensorMem': self.SensorMem,
            'ActuatorTags': self.ActuatorTags,
            'ActuatorMem': self.ActuatorMem,
            'Actuator_String': self.Actuator_String,
            'Sensor_String': self.Sensor_String
        }
        options.update(kwargs)

        # Parse actuator configuration string if provided.
        if len(options['Actuator_String']) > 1:
            Act_Keys = options['Actuator_String'].split(',')
            options['ActuatorTags'] = Act_Keys[::2]
            Act_Mem_Strings = Act_Keys[1::2]
            options['ActuatorMem'] = [int(n) for n in Act_Mem_Strings]
            options['actuator'] = True

        # Parse sensor configuration string if provided.
        if len(options['Sensor_String']) > 1:
            Sen_Keys = options['Sensor_String'].split(',')
            options['SensorTags'] = Sen_Keys[::2]
            Sen_Mem_Strings = Sen_Keys[1::2]
            options['SensorMem'] = [int(n) for n in Sen_Mem_Strings]
            options['sensor'] = True

        self.Time_Mem = options['Time_Mem']
        self.Scan_Time = options['Scan_Time']
        self.SensorTags = options['SensorTags']
        self.SensorMem = options['SensorMem']
        self.ActuatorTags = options['ActuatorTags']
        self.ActuatorMem = options['ActuatorMem']
        self.actuator = options['actuator']
        self.sensor = options['sensor']

    def Agent(self):
        """
        Main communication agent that runs in a separate thread.
        
        Functionality:
            - Connects to the PLC.
            - For sensors: Reads values from Data_Repo and writes them to the PLC.
            - For actuators: Reads values from the PLC and sends them to the server via a ZMQ PUSH socket.
            - Enforces a delay between scans based on Scan_Time.
            - Terminates when the Event is set.
        
        Inputs:
            None (operates on instance attributes).
        
        Outputs:
            Communicates with the PLC and server; terminates by calling sys.exit(0).
        """
        if self.sensor:
            Sensor_data = [0.0] * len(self.SensorTags)
        if self.actuator:
            Actuator_data = [0.0] * len(self.ActuatorTags)
        
        self.PLC.connect()

        if self.actuator:
            context = zmq.Context()
            DB = context.socket(zmq.PUSH)
            serverAddress = self.serAdd.get(block=True)
            DB.connect("tcp://" + serverAddress + ":5555")
            logging.info("Successfully connected to server: " + serverAddress)
        
        time_end = time.time()

        while not self.Event.is_set():
            if self.sensor:
                with self.Lock:
                    for i in range(len(self.SensorTags)):
                        Sensor_data[i] = self.Data.read(self.SensorTags[i])
                    Time_stamp = self.Data.read("Time")
                
                for i in range(len(self.SensorTags)):
                    self.PLC.write(int(self.SensorMem[i]), Sensor_data[i])
                
                if self.Time_Mem != -1:
                    self.PLC.write(int(self.Time_Mem), Time_stamp)
            
            if self.Scan_Time != 0:
                time1 = 0
                while time1 < self.Scan_Time and not self.Event.is_set():
                    time1 = time.time() - time_end
            
            if self.actuator:
                for i in range(int(len(self.ActuatorTags))):
                    Actuator_data[i] = self.PLC.read(int(self.ActuatorMem[i]))
                    if Actuator_data[i] is not None:
                        acutation_signal = bytes(self.ActuatorTags[i] + ":" + str(Actuator_data[i]) + " ", 'utf-8')
                        DB.send(acutation_signal, zmq.NOBLOCK)
                    else:
                        print("Read Failure on IP: %s" % self.PLC.ip)

            time_end = time.time()
        
        self.PLC.close()
        if self.actuator:
            DB.close()
        logging.info('Thread stopped for PLC IP:%s' % self.PLC.ip)
        sys.exit(0)

    def run(self):
        """
        Start the Agent function in a new daemon thread.

        Inputs:
            None
        
        Outputs:
            Creates and starts a thread running Agent.
        """
        self.thread = threading.Thread(target=self.Agent)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        """
        Signal the Agent thread to stop and wait for its termination.

        Inputs:
            None
        
        Outputs:
            Stops the communication thread.
        """
        self.Event.set()
        self.thread.join()

    def wait(self):
        """
        Block until the Agent thread finishes.

        Inputs:
            None
        
        Outputs:
            Joins the Agent thread.
        """
        self.thread.join()

    def __repr__(self):
        """
        Return a string representation of the Connector instance.

        Returns:
            A string showing key attributes of the Connector.
        """
        return "Connector('{},{},{},{},{}')".format(self.PLC, self.serAdd, self.Data, self.Lock, self.Event)
               

def UDP_Client(Data, Event, serAdd, Lock, nPLCs):
    """
    UDP_Client receives UDP messages, extracts tag data, and updates the Data_Repo.

    Inputs:
        Data: An instance of Data_Repo.
        Event: A threading.Event used to signal when to stop the UDP client.
        serAdd: A queue for storing the server address (to be shared with Connector instances).
        Lock: A threading.Lock used for synchronizing access to Data.
        nPLCs (int): The number of PLCs (used to distribute the server address).
    
    Functionality:
        - Binds a UDP socket to a broadcast address and listens for incoming messages.
        - On the first message, distributes the server IP to all PLC threads.
        - Parses each incoming message to update tag values and a time stamp in Data_Repo.
        - Checks for a "STOP" command to terminate.
    
    Outputs:
        Continuously updates the Data_Repo until a stop event is received, then exits.
    """
    bufferSize = 128 * 1000
    serverAddressPort = ("255.255.255.255", 8000)
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPClientSocket.bind(serverAddressPort)
    UDPClientSocket.settimeout(30)

    with Lock:
        Tags = Data.UDP_TAGS()

    nTags = len(Tags)
    Values = [0.0] * nTags
    Time_Stamp = 0.0
    First_Time = True

    while not Event.is_set():
        msgFromServer, address = UDPClientSocket.recvfrom(bufferSize)
        if First_Time:
            for i in range(nPLCs):
                serAdd.put(address[0])
            First_Time = False

        msg = str(msgFromServer, 'UTF-8')
        msg_split = msg.split()

        if msg_split[0] == "STOP":
            Event.set()
            logging.info("UDP Client was sent stop request from DataBroker.")
            break

        for i in range(nTags):
            try:
                IDX = msg_split.index(Tags[i])
                Values[i] = float(msg_split[IDX + 1])
                Time_Stamp = float(msg_split[IDX + 2])
            except:
                logging.info("Tag: %s not in UDP message..." % Tags[i])
        
        with Lock:
            for i in range(nTags):
                Data.write(Tags[i], Values[i])
                Data.write("Time", Time_Stamp)

    UDPClientSocket.close()
    logging.info("UDP Client received event. Exiting")
    sys.exit(0)


if __name__ == "__main__":
    """
    Main entry point of the program.

    Functionality:
        - Configures logging.
        - Sets up inter-thread communication queues, an event, and a lock.
        - Initializes the system configuration by calling the initialization() function.
        - Creates instances of Data_Repo, MB_PLC, and Connector for each PLC.
        - Starts the UDP client thread and Connector threads.
        - Waits for threads to complete, handling KeyboardInterrupt for graceful shutdown.
    
    Inputs:
        None (configuration is received at runtime).
    
    Outputs:
        Launches the multi-threaded system for PLC communication and data updating.
    """
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    pipeline = queue.Queue(maxsize=100)
    serverAddress = queue.Queue(maxsize=10)

    event = threading.Event()
    Lock = threading.Lock()

    (IP_PLCs, nPLC, Sensor_Loc, Actuator_Loc, Byte_order, Word_order,
     Mem_Format, Port, Time_Mem_I, scanTime_I, Sensor_Tags, Sensor_Mem,
     Actuator_Tags, Actuator_Mem, data) = initialization()

    Sensor_Data = Data_Repo(data[2])
    Sensor_Data.write("Time", 0.0)

    PLC = []
    Comms = []

    for i in range(nPLC):
        PLC.append(MB_PLC(IP_PLCs[i], Port[i]))
        PLC[i].Mem_default = Mem_Format[i]
        
        if Byte_order[i].lower() == 'little':
            PLC[i].byteOrder = Endian.LITTLE
        else:
            PLC[i].byteOrder = Endian.BIG
        
        if Word_order[i].lower() == 'little':
            PLC[i].wordOrder = Endian.LITTLE
        else:
            PLC[i].wordOrder = Endian.BIG

        Comms.append(Connector(PLC[i], serverAddress, Sensor_Data, Lock, event))

        if nPLC == 1:
            S_Tags = Sensor_Tags
            S_Mem = Sensor_Mem
            Sen = True if S_Tags is not None else False
            A_Tags = Actuator_Tags
            A_Mem = Actuator_Mem
            Act = True if A_Tags is not None else False
        else:
            if Sensor_Loc[i] != 0:
                Sen = True
                S_IDX = sum(Sensor_Loc[0:i])
                S_Tags = Sensor_Tags[S_IDX:(S_IDX + Sensor_Loc[i])]
                S_Mem = Sensor_Mem[S_IDX:(S_IDX + Sensor_Loc[i])]
            else:
                Sen = False
                S_Tags = None
                S_Mem = -1

            if Actuator_Loc[i] != 0:
                Act = True
                A_IDX = sum(Actuator_Loc[0:i])
                A_Tags = Actuator_Tags[A_IDX:(A_IDX + Actuator_Loc[i])]
                A_Mem = Actuator_Mem[A_IDX:(A_IDX + Actuator_Loc[i])]
            else:
                Act = False
                A_Tags = None
                A_Mem = -1
            
        Comms[i].Set(Time_Mem=Time_Mem_I[i],
                     Scan_Time=scanTime_I[i],
                     actuator=Act,
                     sensor=Sen,
                     SensorTags=S_Tags,
                     SensorMem=S_Mem,
                     ActuatorTags=A_Tags,
                     ActuatorMem=A_Mem)
        
    UDP_Thread = threading.Thread(target=UDP_Client, args=(Sensor_Data, event, serverAddress, Lock, nPLC))
    UDP_Thread.daemon = True

    try:
        UDP_Thread.start()
        for c in Comms:
            c.run()

        UDP_Thread.join()
        time.sleep(2)
    
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            event.set()
            sys.exit(0)
        except SystemExit:
            event.set()
            os._exit(0)

