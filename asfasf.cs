public override object Invoke( UserSession session, Object inputParams )
{
    var type = inputParams as string;

    var items = new List<object>();
    var io_dir = Service.GetInfoObject(Guid.Parse("2474b775-8b98-413d-b9e1-20f744a4ec88"));
    var containers = io_dir.GetValue<DataContainer[]>("SearchСontainers");

    if (containers.Count() == 0)
        return items;

    var searchOperation = new SearchOperation(EntityIdentifier.InfoObject);
    searchOperation.IsCaseSensitive = false;
    var template1 = Service.GetTemplate( AttributableEntityIdentifier.InfoObject, @"InfoObjects\STD\Documents\Document\TechnicalDocument" );
    var template2 = Service.GetTemplate( AttributableEntityIdentifier.InfoObject, @"InfoObjects\STD\Documents\DocumentVersion\TechnicalDocumentVersion" );
    var template3 = Service.GetTemplate( AttributableEntityIdentifier.InfoObject, @"InfoObjects\BASIC\Files\File" );

    searchOperation.Containers.AddRange( containers.SkipNull() );
    searchOperation.IncludeInheritedContainers = true;
    
    SearchExpressionItem rootItem;
    SearchExpressionItem subItem;
    {
        var templateFilter1 = new SearchTemplateFilterItem(  );
        templateFilter1.FilterByTemplate = template1;
        templateFilter1.IncludeInherited = true;
    
        {
            var andOp2 = new LogicalAndOperationItem( templateFilter1 );
    
            {
                var orOp3 = new LogicalOrOperationItem( andOp2 );

                var attrFilter4 = new SearchAttributeCriterionItem( orOp3 );
                attrFilter4.Attribute = template1.GetAttributeDef( "ProductName" ).ModifiedClone;
                attrFilter4.OperatorId = RelationalOperator.Contains;
                attrFilter4.Argument1 = type;
            }
    
            {
                var attrFilter3 = new SearchAttributeCriterionItem( templateFilter1 );
                attrFilter3.Attribute = template1.GetAttributeDef( "LifeCycleState" ).ModifiedClone;
                attrFilter3.OperatorId = RelationalOperator.Equal;
                attrFilter3.Argument1 = Service.GetObject( EntityIdentifier.InfoObject, 1666 );
            }
    
            {
                var attrFilter3 = new SearchAttributeCriterionItem( andOp2 );
                attrFilter3.Attribute = template1.GetAttributeDef( "ActualVersion" ).ModifiedClone;
                attrFilter3.OperatorId = RelationalOperator.Condition;
    
                {
                    var templateFilter4 = new SearchTemplateFilterItem(  );
                    templateFilter4.FilterByTemplate = template2;
                    templateFilter4.IncludeInherited = true;
    
                    {
                        var attrFilter5 = new SearchAttributeCriterionItem( templateFilter4 );
                        attrFilter5.Attribute = template3.GetAttributeDef( "Body" ).ModifiedClone;
                        attrFilter5.OperatorId = RelationalOperator.Contains;
                        attrFilter5.Argument1 = @".cdw";
                    }
                    subItem = templateFilter4;
                }
    
                attrFilter3.Argument1 = subItem;
            }
        }
        rootItem = templateFilter1;
    }
    
    searchOperation.SearchExpressionTree = rootItem;
    
    searchOperation.Execute( false );
    var resultObjects = searchOperation.FoundObjects;
    
    foreach(InfoObject io in resultObjects)
    {
        var ai = io.GetValue<InfoObject>("ActualVersion");
        IEnumerable<CollectionElement> DocumentViewContent = ai.GetAttribute("DocumentViewContent").CollectionElements;        
        if (DocumentViewContent.Count() == 0)
            continue;
        
        items.Add(new {
            id = ai.Id,
            name = io.ToString(),
            link = Service.UI.FormatLink(io, null),        
        });
    }

    return items.ToArray();
}
