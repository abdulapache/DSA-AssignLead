using System.Data;
using System.Data.SqlClient;

namespace WithdarwLead
{
    class Program
    {
        static string connectionstring = "Server=ec2-18-189-230-198.us-east-2.compute.amazonaws.com;Database=gosales;User Id= WSUsr; Password=1234!@#$; TrustServerCertificate=True";


        static Timer timer;

        static void Main(string[] args)
        {
            // Start the timer to execute the program every 30 minutes
            SetTimer();

            // Keep the program running
            Console.ReadLine();
        }

        static void SetTimer()
        {
            // Calculate the time until the next 30-minute interval
            DateTime now = DateTime.Now;
            DateTime nextRunTime = now.AddHours(2 - now.Minute % 2).AddMinutes(-now.Minute).AddSeconds(-now.Second);

            // Check if it's Sunday, if yes, set the next run time to next Monday
            if (nextRunTime.DayOfWeek == DayOfWeek.Sunday)
            {
                nextRunTime = nextRunTime.AddDays(1);
            }

            // Calculate the delay until the next run time
            TimeSpan delay = nextRunTime - now;

            // Create a timer to trigger the program execution
            timer = new Timer(ExecuteProgram, null, delay, TimeSpan.FromHours(2));
        }

        static void ExecuteProgram(object state)
        {

            var dsa = GoSalesAgentList();
            if (dsa != null)
            {
                foreach (DataRow row in dsa.Rows)
                {
                    SqlDataAdapter ad = new SqlDataAdapter();
                    DataTable dt = new DataTable();

                    var userId = Convert.ToInt32(row[0].ToString());

                    if (userId > 0)
                    {
                        int leadCount = GetLeadCount(userId);
                        var totalAssginLead = 100 - leadCount;
                        if (totalAssginLead <= 100 && totalAssginLead >= 0)
                        {
                            SqlConnection conn = new SqlConnection(connectionstring);
                            conn.Open();
                            SqlCommand cmd = conn.CreateCommand();
                            cmd.CommandText = "SELECT TOP (100) Email, Phone, FirstName, LastName, SourceEmail, id FROM FreshLeadsTbl WHERE isdeployed = 0 AND SourceProject IN ('Arabia Business Investors - 91,103 contacts.xlsx') AND Phone NOT IN (SELECT phone FROM Customers) and Phone NOT IN(Select Number from DncrData where IsDncr = 1)";
                            //cmd.Parameters.AddWithValue("@totalAssginLead", totalAssginLead);
                            ad.SelectCommand = cmd;
                            ad.Fill(dt);

                            // upload this cutomer

                            conn.Close();
                            var c = assignLead(dt, userId);
                            Console.WriteLine(c + " Lead Assign to AgentId:" + userId);

                            dt = new DataTable();
                            cmd.Parameters.Clear();
                        }
                        else
                        {
                            Console.WriteLine("All leads have already been assigned to agents.");
                        }
                    }
                }
            }
            int GetLeadCount(int agentId)
            {
                using (SqlConnection conn = new SqlConnection(connectionstring))
                {
                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand("SELECT COUNT(*) FROM LeadEngine WHERE CurrentAssignTo = @AgentId AND ProcessedStatus = 0", conn))
                    {
                        cmd.Parameters.AddWithValue("@AgentId", agentId);
                        return (int)cmd.ExecuteScalar();
                    }
                }
            }
            int assignLead(DataTable dt, int agentId)
            {
                var count = 0;
                if (dt.Rows.Count > 0)
                {
                    foreach (DataRow item in dt.Rows)
                    {
                        try
                        {
                            String FirstName = string.Empty;
                            string LastName = string.Empty;
                            string Phone = string.Empty;
                            string Email = string.Empty;
                            string SourceEmail = string.Empty;
                            string FlLead = string.Empty;

                            Email = item[0].ToString() ?? string.Empty;
                            Phone = item[1].ToString() ?? string.Empty;
                            FirstName = item[2].ToString() ?? string.Empty;
                            LastName = item[3].ToString() ?? string.Empty;
                            SourceEmail = item[4].ToString() ?? string.Empty;


                            var re = checkEmailPhone(Phone, Email);
                            if (re == 0)
                            {
                                SqlConnection con = new SqlConnection(connectionstring);
                                con.Open();

                                FlLead = item[5].ToString() ?? "0";
                                using (SqlCommand cmd = new SqlCommand())
                                {
                                    cmd.Connection = con;
                                    string insertQuery = "INSERT INTO Customers ([Email], [FirstName], [LastName], [LeadSourceEmail], [Phone]) VALUES (@Email, @FirstName, @LastName, @SourceEmail, @Phone); SELECT SCOPE_IDENTITY();";
                                    cmd.CommandText = insertQuery;
                                    cmd.Parameters.AddWithValue("@Email", Email);
                                    cmd.Parameters.AddWithValue("@FirstName", FirstName);
                                    cmd.Parameters.AddWithValue("@LastName", LastName);
                                    cmd.Parameters.AddWithValue("@SourceEmail", SourceEmail);
                                    cmd.Parameters.AddWithValue("@Phone", Phone);
                                    int customerId = Convert.ToInt32(cmd.ExecuteScalar());
                                    con.Close();

                                    con.Open();
                                    string insertQuery1 = "INSERT INTO LeadEngine ([CustomerID], [CurrentAssignTo], [IsActive], [LeadBy], [ProcessedStatus], [AssignDate], [FreshLeadId]) VALUES (@CustomerId, @AgentId, 1, 0, 0, GETDATE(), @FlLead)";
                                    cmd.CommandText = insertQuery1;
                                    cmd.Parameters.Clear(); // Clear previous parameters
                                    cmd.Parameters.AddWithValue("@CustomerId", customerId);
                                    cmd.Parameters.AddWithValue("@AgentId", agentId);
                                    cmd.Parameters.AddWithValue("@FlLead", FlLead);
                                    cmd.ExecuteNonQuery();
                                    con.Close();
                                    count++;
                                }
                            }
                            else
                            {
                                Console.WriteLine("customer assigned already!");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("There is an error on businese uploadFreshLead!");
                            Console.WriteLine(ex.Message);
                        }
                    }
                }
                else
                {
                    Console.WriteLine("All Leads from Fresh-Databse has been deployed!");
                }
                return count;
            }
            int checkEmailPhone(string phone, string email)
            {
                var res = 0;
                SqlConnection con = new SqlConnection(connectionstring);
                con.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = con;
                string query = "SELECT COUNT(*) FROM Customers WHERE phone = @Phone";
                cmd.CommandText = query;
                cmd.Parameters.AddWithValue("@Phone", phone); // Assuming 'phone' is a string variable
                object result = cmd.ExecuteScalar();
                if (result != null && result != DBNull.Value)
                {
                    int count = Convert.ToInt32(result);
                    if (count > 0)
                    {
                        Console.WriteLine("Email and Phone already exist: Mobile:" + phone + "-Email:" + email);
                        res = 1;
                    }
                }
                con.Close();
                return res;
            } 
            int checkDNCR(string phone)
            {
                var Dncrres = 0;
                SqlConnection con = new SqlConnection(connectionstring);
                con.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = con;
                string query = "SELECT COUNT(*) FROM DncrData WHERE phone = @Phone and IsDncr = 1";
                cmd.CommandText = query;
                cmd.Parameters.AddWithValue("@Phone", phone); // Assuming 'phone' is a string variable
                object dncrResult = cmd.ExecuteScalar();
                if (dncrResult != null && dncrResult != DBNull.Value)
                {
                    int count = Convert.ToInt32(dncrResult);
                    if (count > 0)
                    {
                        Console.WriteLine("Number is DNCR" + phone);
                        Dncrres = 1;
                    }
                }
                con.Close();
                return Dncrres;
            }
            DataTable GoSalesAgentList()
            {
                DataTable dt = new DataTable();
                SqlDataAdapter adapter = new SqlDataAdapter();
                try
                {
                    SqlConnection con = new SqlConnection();
                    con.ConnectionString = connectionstring;
                    con.Open();
                    SqlCommand cmd = new SqlCommand();
                    cmd.Connection = con;
                    cmd.CommandText = "select Id from BusinessUsers where IsActive=1 and Roles IN('MM2','MM1','MM3')";
                    adapter.SelectCommand = cmd;
                    adapter.Fill(dt);
                    con.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("There is an error on businese user fetching!");
                    Console.WriteLine(ex.Message);
                }
                return dt;
            }

            Console.WriteLine("Last Excute" + DateTime.Now.ToString());

        }



    }
}