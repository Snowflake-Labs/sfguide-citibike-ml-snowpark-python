def snowpark_connect(state_file='./include/state.json'):
    import snowflake.snowpark as snp
    import json
    
    with open(state_file) as sdf:
        state_dict = json.load(sdf)    
    
    session=None
    session = snp.Session.builder.configs(state_dict["connection_parameters"]).create()
    session.use_warehouse(state_dict['compute_parameters']['default_warehouse'])
    return session, state_dict
