Не удалось привести тип объекта "System.Drawing.Bitmap" к типу "System.Drawing.Imaging.Metafile"

try{
      var tp = new InfoObject( (DataContainer)null, @"InfoObjects\StartScreen\TypicalParts" );
      var mf = Service.GetScriptingTask( "GetDrawingID" ).Invoke(urlArgs.Form["id"]) as Image;
      tp["Preview"] = mf;
      
      Service.UI.ShowObjectProperties( tp, false );
  }catch (Exception e){
      Service.UI.ShowMessage(e.Message);
  }
