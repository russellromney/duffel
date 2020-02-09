
def _concat(values,axis=0, ignore_index=False):
    for item in values:
        assert any( type(item).__name__=='_DuffelDataFrame', type(item).__name__=='_DuffelCol'), f"duffel.concat values must be type duffel.Col or duffel.DataFrame, not {type(item)}"
    
    # check index overlap
    if not ignore_index:
        assert len(set([x.index for x in values ])) == '' # list of all values
    else:
        pass

    return ''
        
    
    # add columns that are not shared

