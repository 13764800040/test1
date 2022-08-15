using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Fleck;
using Newtonsoft.Json.Linq;
using System.Data.SqlClient;

namespace ConsoleApplication3
{
    class Program
    {
        static void Main(string[] args)
        {
            FleckLog.Level = LogLevel.Debug;
            List<IWebSocketConnection> sockets = new List<IWebSocketConnection>();
            WebSocketServer server = new WebSocketServer("ws://machinestream.herokuapp.com/ws");
            server.Start(socket =>
            {
                socket.OnOpen = () =>
                {
                    Console.WriteLine("Socket Open");
                    sockets.Add(socket);
                };
                socket.OnClose = () =>
                {
                    Console.WriteLine("Socket Close");
                    sockets.Remove(socket);
                };
                socket.OnMessage = message => {
                    Console.WriteLine("Socket Receive Message:" + message);
                    JObject jOb = null;
                    try
                    {
                        jOb = JObject.Parse(message);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Message Parse Exception:" + ex.ToString());
                        socket.Send(GetResponse("Request Param Parse Error.").ToString());
                        return;
                    }
                    if(SaveToDb("sqlConnectionStr", jOb))
                    {
                        socket.Send(GetResponse("").ToString());
                    }
                    else
                    {
                        socket.Send(GetResponse("Save To DB Error").ToString());
                    }
                };
            });

            Console.ReadLine();
        }

        static JObject GetResponse(string errorMessage)
        {
            JObject ret = new JObject();
            if (string.IsNullOrWhiteSpace(errorMessage))
            {
                ret.Add("Result", "Success");
                ret.Add("Message", "Save DB Success");
            }
            else
            {
                ret.Add("Result", "Error");
                ret.Add("Message", "Save DB Error: " + errorMessage);
            }
            return ret;
        }

        static bool SaveToDb(string conStr, JObject jOb)
        {
            SqlConnection sql = new SqlConnection(conStr);
            try
            {
                sql.Open();
                string fileds = "";
                string values = "";
                SqlCommand command = new SqlCommand("", sql);
                GetCommand(command, jOb, "topic", ref fileds, ref values);
                GetCommand(command, jOb, "ref", ref fileds, ref values);
                GetCommand(command, jOb, "payload.machine_id", ref fileds, ref values);
                GetCommand(command, jOb, "payload.id", ref fileds, ref values);
                GetCommand(command, jOb, "payload.timestamp", ref fileds, ref values);
                GetCommand(command, jOb, "payload.status", ref fileds, ref values);
                GetCommand(command, jOb, "event", ref fileds, ref values);

                command.CommandText = "Insert Into table(" + fileds.TrimEnd(',') + ") values(" + values.TrimEnd(',') + ");";

                command.ExecuteNonQuery();
                sql.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Message Save To DB Exception:" + ex.ToString());
                return false;
            }
            return true;
        }

        static void GetCommand(SqlCommand command, JObject jOb, string key, ref string fileds, ref string values)
        {
            int index = key.LastIndexOf(".");
            if(index >= 0)
            {
                string v = key.Substring(0, index);
                string realKey = key.Substring(index + 1);
                command.Parameters.Add(new SqlParameter("@" + realKey, (string)jOb[v][realKey]));
                fileds += realKey + ", ";
                values += "@" + realKey + ",";
            }
            else
            {
                command.Parameters.Add(new SqlParameter("@" + key, (string)jOb[key]));
                fileds += key + ", ";
                values += "@" + key + ",";
            }
        }
    }
}
