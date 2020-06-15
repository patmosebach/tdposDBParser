using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace tdposDBParser
{
    class Program
    {
        static void Main(string[] args)
        {

            string fileContents = File.ReadAllText("C:/src/tdposDBParser/tdposDBParser/EventsImporter.php");

            string[] delimiter = new string[1];

            delimiter[0] = @"$query = """;

            string[] firstSplit = fileContents.Split(delimiter, new StringSplitOptions());

            string[] secondSplit = new string[firstSplit.Length - 1];

            string[] secondDelimiter = new string[1];

            delimiter[0] = @";";

            for (int i = 0; i < secondSplit.Length; i++)
            {
                if (i == 0)
                {
                }
            }
            string result = firstSplit[1].ToString();
            fileContents.ToLower();
        }
    }
}
