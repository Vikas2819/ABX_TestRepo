using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace ABXProject_V1
{
    public class Program
    {
        private const string HostName = "localhost"; 
        private const int Port = 3000; 
        static void Main(string[] args)
        {   
            try
            {
                //Using TCP Client to connect to the server
                using (TcpClient Client =new TcpClient(HostName, Port))
                {
                    Console.WriteLine("Connection Successfull");
                    NetworkStream mStream= Client.GetStream();

                    List<DataPacket> mPackets = StreamAllPackets(mStream); //Stream All Packets (Call Type 1)

                    mPackets = HandleMissingPackets(mPackets); //Handle Missing Packets

                    SaveToJson(mPackets); //Save the Data into json file                
                }
            }
            catch (Exception Ex)
            {
                Console.WriteLine("Error Occured=" + Ex.ToString());
            }
        }

        public static List<DataPacket> StreamAllPackets(NetworkStream vStream)
        {
            SendRequest(vStream, 1);
            List<DataPacket> mDataPacket=new List<DataPacket>();

            using (BinaryReader mReader=new BinaryReader(vStream))
            {
                try
                {
                    while (true) 
                    {
                        DataPacket mPacket = DataPacket.ReadPacket(mReader);
                        mDataPacket.Add(mPacket);
                        Console.WriteLine($"Received Packet: Symbol={mPacket.Symbol}, Buy/Sell={mPacket.BuySellIndicator}, Quantity={mPacket.Quantity}, Price={mPacket.Price}, Sequence={mPacket.Sequence}");
                    }
                }
                catch(Exception Ex)
                {
                    Console.WriteLine("Stream All Packets Failed.Server Disconnected due to :" +Ex.ToString());
                }
            }    
            return mDataPacket; 
        }

        public static List<DataPacket> HandleMissingPackets(List<DataPacket> vDataPackets)
        {
            HashSet<int> mRecievedSequences= new HashSet<int>();    
            foreach(var mPacket in vDataPackets)
            {
                mRecievedSequences.Add(mPacket.Sequence);
            }

            int mMaxSequence = vDataPackets[vDataPackets.Count-1].Sequence;

            for(int i = 1; i <= mMaxSequence; i++)
            {
                if (!mRecievedSequences.Contains(i))
                {
                    Console.WriteLine("Missing Sequence Found:{0}" + i.ToString());
                    DataPacket mMissingPacket = RequestResendPacket(i);
                    vDataPackets.Add(mMissingPacket);
                }
            }
            vDataPackets.Sort((x, y) => x.Sequence.CompareTo(y.Sequence)); //Sort the packets in order
            return vDataPackets;    
        }

        public static DataPacket RequestResendPacket(int vSequenceNumber)
        {
            using (TcpClient mClient=new TcpClient(HostName, Port))
            {
                NetworkStream mStream = mClient.GetStream();
                Console.WriteLine("Requesting Missing Packet");

                SendRequest(mStream,2,(byte)vSequenceNumber); // Send Resend Packet Requet

                using (BinaryReader mReader=new BinaryReader(mStream))
                {
                    DataPacket mDataPacket=DataPacket.ReadPacket(mReader);
                    Console.WriteLine("Recieved ReSent Packet");
                    return mDataPacket; 
                }

            }
        }
        public static void SendRequest(NetworkStream vStream,byte vCallType,byte vResendReq=0)
        {
            BinaryWriter mWriter = new BinaryWriter(vStream);
            
            mWriter.Write(vCallType);  // Call Type (1 byte)
            mWriter.Write(vResendReq); // Resend Sequence(1 byte,0 for call type 1)  
            mWriter.Flush();           
        }

        public static void SaveToJson(List<DataPacket> vDataPackets)
        {
            string mFileName = "ABXData.json";
            string mFilePath = Path.Combine(Directory.GetCurrentDirectory(),mFileName);

            string mJson = JsonConvert.SerializeObject(vDataPackets, Formatting.Indented);

            File.WriteAllText(mFilePath, mJson);

            Console.WriteLine("Data Saved to Path :" + mFilePath);
        }

        public class DataPacket
        {
            public string Symbol { get; set; }
            public char BuySellIndicator { get; set; }
            public int Quantity { get; set; }
            public int Price { get; set; }
            public int Sequence { get; set; }

            //For Big Endian
            public static DataPacket ReadPacket(BinaryReader reader)
            {
                DataPacket packet = new DataPacket
                {
                    Symbol = Encoding.ASCII.GetString(reader.ReadBytes(4)),
                    BuySellIndicator = (char)reader.ReadByte(),
                    Quantity = ReadBigEndianInt32(reader),
                    Price = ReadBigEndianInt32(reader),
                    Sequence = ReadBigEndianInt32(reader)
                };

                return packet;
            }

            private static int ReadBigEndianInt32(BinaryReader reader)
            {
                byte[] bytes = reader.ReadBytes(4);
                Array.Reverse(bytes);
                return BitConverter.ToInt32(bytes, 0);
            }
        }
    }
}
