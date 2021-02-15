using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Microsoft.Azure.Cosmos;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Scripts;

namespace ChyaAzureCosmosSQL
{
   class Program
   {
      static void Main(string[] args)
      {
         Console.WriteLine("CHYA Auzre CosmosDB SQL Test...");

         string dbName = "ChyaTestDB";
         var connStr = ConfigurationManager.AppSettings["CosmosConn"];
         var token = ConfigurationManager.AppSettings["CosmosAuth"];

         var dbResponse = CreateDb(connStr, token, dbName);

         string containerName = "TestContainer";
         //var containerResponse = CreateContainer(dbResponse, containerName);

         var container = SelectContainer(dbResponse, containerName);

         CreateDoc(container);

         FindByLinq(container, "Stockholm");
         var task = FindBySql(container, "Lund");
         Task.WaitAll(task);

         var task2 = UpsertDoc(container, "BBB");
         Task.WaitAll(task2);

         var task5 = FindDevice(container, "BBB");
         Task.WaitAll(task5);

         var task3 = ReplaceDoc(container, "DDD");
         Task.WaitAll(task3);

         var task4 = DeleteDocAsync(container, "Lund");
         Task.WaitAll(task4);

         Console.WriteLine("Press any key to quit...");
         Console.ReadKey();
      }

      private static Container SelectContainer(DatabaseResponse dbResponse, string containerName)
      {
         return dbResponse.Database.GetContainer(containerName);
      }

      static DatabaseResponse CreateDb(string connStr, string token, string dbName)
      {
         
         var client = new CosmosClient(connStr, token);

         var dbRes = client.CreateDatabaseIfNotExistsAsync(dbName);

         Task.WaitAny(dbRes);

         var msg = "";
         if (dbRes.Result.StatusCode == HttpStatusCode.Created)
         {
            msg = $"DB {dbName} created.";
         }
         else if (dbRes.Result.StatusCode == HttpStatusCode.OK)
         {
            msg = $"DB {dbName} already exists, won't create again.";
         }

         Console.WriteLine(msg);

         return dbRes.Result;

      }

      static ContainerResponse CreateContainer(DatabaseResponse dbResponse, string containerName)
      {

         //Partition key defines by path "/"
         var conRes = dbResponse.Database.CreateContainerIfNotExistsAsync(containerName, "/City");

         var msg = "";
         if (conRes.Result.StatusCode == HttpStatusCode.Created)
         {
            msg = $"Container {containerName} created, partitionKey /City.";
         }
         else if (conRes.Result.StatusCode == HttpStatusCode.OK)
         {
            msg = $"Container {containerName} already exists, won't create again.";
         }

         Console.WriteLine(msg);

         return conRes.Result;
      }

      private static void CreateDoc(Container container)
      {
         var testPerson1 = new ChyaTestPerson()
                          {
                             id = "1",
                             City = "Stockholm",
                             PersonName = "AAA",
                             Devices = new List<ChyaTestDevice>()
                                       {
                                          new ChyaTestDevice()
                                          {
                                             DeviceName = "iPad mini 5",
                                             OSName = "iOS 14.4"
                                          },
                                          new ChyaTestDevice()
                                          {
                                             DeviceName = "S20",
                                             OSName = "Android 11"
                                          },
                                       }
                          };
         var res1 = CreateDocIfNotExists(container, testPerson1);
         Console.WriteLine($"Checking {testPerson1.PersonName}");

         var testPerson2 = new ChyaTestPerson()
                           {
                              id = "2",
                              City = "Stockholm",
                              PersonName = "BBB",
                              Devices = new List<ChyaTestDevice>()
                                        {
                                           new ChyaTestDevice()
                                           {
                                              DeviceName = "iPad Pro",
                                              OSName = "iOS 14.4"
                                           },
                                           new ChyaTestDevice()
                                           {
                                              DeviceName = "iPhone 12",
                                              OSName = "iOS 14.4"
                                           },
                                        }
                           };

         var res2 = CreateDocIfNotExists(container, testPerson2);
         Console.WriteLine($"Checking {testPerson2.PersonName}");

         var testPerson3 = new ChyaTestPerson()
                           {
                              id = "3",
                              City = "Stockholm",
                              PersonName = "CCC",
                              Devices = new List<ChyaTestDevice>()
                                        {
                                           new ChyaTestDevice()
                                           {
                                              DeviceName = "iPad",
                                              OSName = "iOS 14.4"
                                           },
                                           new ChyaTestDevice()
                                           {
                                              DeviceName = "S21",
                                              OSName = "Android 11"
                                           },
                                        }
                           };

         var res3 = CreateDocIfNotExists(container, testPerson3);
         Console.WriteLine($"Checking {testPerson3.PersonName}");

         var testPerson4 = new ChyaTestPerson()
                           {
                              id = "4",
                              City = "Stockholm",
                              PersonName = "DDD",
                              Devices = new List<ChyaTestDevice>()
                                        {
                                           new ChyaTestDevice()
                                           {
                                              DeviceName = "iPad Pro",
                                              OSName = "iOS 14.4"
                                           }
                                        }
                           };

         var res4 = CreateDocIfNotExists(container, testPerson4);
         Console.WriteLine($"Checking {testPerson4.PersonName}");

         //Wait for finish
         Task.WaitAll(res1, res2, res3, res4);
      }

      public static async Task<ItemResponse<T>> CreateDocIfNotExists<T>(Container container, T doc) where T : ChyaTestPerson
      {
         ItemResponse<T> res = null;
         try
         {
            res = await container.ReadItemAsync<T>(doc.id, new PartitionKey(doc.City));
            if (res.StatusCode == HttpStatusCode.OK)
            {
               Console.WriteLine($"{doc.PersonName} already exists.");
            }
         }
         catch (CosmosException e)
         {
            if (e.StatusCode == HttpStatusCode.NotFound)
            {
               res = await container.CreateItemAsync<T>(doc, requestOptions: new ItemRequestOptions()
                                                                             {
                                                                                PostTriggers = new List<string>()
                                                                                               {
                                                                                                  "addPostTrigger"
                                                                                               },
                                                                                PreTriggers = new List<string>()
                                                                                              {
                                                                                                 "dateTime"
                                                                                              }
                                                                             }, 
                                                        partitionKey:new PartitionKey("Stockholm"));

               Console.WriteLine($"{doc.PersonName} created.");
            }
            else
            {
               throw;
            }
         }

         return res;
      }

      static void FindByLinq(Container container, string city)
      {
         Console.WriteLine($"Search person in {city} by Linq");

         var query = container.GetItemLinqQueryable<ChyaTestPerson>(true);
         var res = query.Where(x => x.City == city).ToList();

         foreach (var person in res)
         {
            Console.WriteLine(person.ToString());
         }

      }

      static async Task FindBySql(Container container, string city)
      {
         Console.WriteLine($"Search person in {city} by SQL");

         //From c Where c.City = 'XXX'; c stands for container
         var iterator = container.GetItemQueryIterator<ChyaTestPerson>($"Select * from ChyaTestPerson Where ChyaTestPerson.City = '{city}'");

         while (iterator.HasMoreResults)
         {
            foreach (var person in await iterator.ReadNextAsync())
            {
               Console.WriteLine(person.ToString());
            }
         }

      }

      static async Task UpsertDoc(Container container, string name)
      {
         Console.WriteLine($"Search person {name} by Linq");

         var query = container.GetItemLinqQueryable<ChyaTestPerson>(true);
         var persons = query.Where(x => x.PersonName == name).ToArray();

         Console.WriteLine($"Updating {name} devices ...");
         persons[0].Devices.Add(new ChyaTestDevice()
                                {
                                   DeviceName = "Mate 40 Pro",
                                   OSName = "Android 11"
                                });

         try
         {
            await container.UpsertItemAsync<ChyaTestPerson>(persons[0]);
         }
         catch (CosmosException e)
         {
            Console.WriteLine(e.Message);
         }

         Console.WriteLine($"Updating {name} finished with Upsert...");

      }

      static async Task FindDevice(Container container, string name)
      {
         Console.WriteLine($"Search person {name}'s devices by SQL");
         var iterator = container.GetItemQueryIterator<ChyaTestDevice>(
            $"Select d.DeviceName From ChyaTestPerson ctp Join d In ctp.Devices where ctp.PersonName = '{name}'");

         while (iterator.HasMoreResults)
         {
            foreach (var device in await iterator.ReadNextAsync())
            {
               Console.WriteLine(device.DeviceName);
            }
         }
      }

      static async Task ReplaceDoc(Container container, string name)
      {
         Console.WriteLine($"Search person {name} by Linq");

         var query = container.GetItemLinqQueryable<ChyaTestPerson>(true);
         var persons = query.Where(x => x.PersonName == name).ToArray();

         Console.WriteLine($"Updating {name} city (partition key) ...");
         persons[0].City = "Wow!Again!";

         try
         {
            await container.ReplaceItemAsync<ChyaTestPerson>(persons[0], persons[0].id);
            Console.WriteLine($"Updating {name} finished with Replace...");
         }
         catch (CosmosException e)
         {
            Console.WriteLine(e.Message);
            Console.WriteLine("You cannot update partition key value");
         }

      }

      static async Task DeleteDocAsync(Container container, string name)
      {
         var getRes = container.GetItemLinqQueryable<ChyaTestPerson>(true).Where(x => x.City == name).ToList();

         foreach (var person in getRes)
         {
            await container.DeleteItemAsync<ChyaTestPerson>(person.id, new PartitionKey(person.City));
            Console.WriteLine($"Data of {person.PersonName} in {person.City} was deleted!");
         }

      }
   }
}
