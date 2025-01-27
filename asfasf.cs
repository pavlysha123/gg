var setting = new Newtonsoft.Json.JsonSerializerSettings()
                {
                    //Culture = new CultureInfo(
                };

                var result = Service.GetScriptingTask( "GetListObjectsDetail" ).Invoke(type).ToString();
                List<object> items = Newtonsoft.Json.JsonConvert.DeserializeObject<List<object>>(result, setting);
                                
                renderArgs["objects"] = items;
