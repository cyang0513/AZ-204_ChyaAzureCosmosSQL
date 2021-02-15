using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace ChyaAzureCosmosSQL
{
   class ChyaTestPerson
   {
      [JsonProperty("id")]
      public string id { get; set;}
      public string PersonName { get; set; }
      public string City { get; set; }
      public List<ChyaTestDevice> Devices { get; set;}

      public override string ToString()
      {
         return JsonConvert.SerializeObject(this);
      }
   }

   class ChyaTestDevice
   {
      public string DeviceName { get; set; }
      public string OSName { get; set; }

      public override string ToString()
      {
         return JsonConvert.SerializeObject(this);
      }

   }
}
