def eval_model_func(input_data: str, 
                    y_true_name: str, 
                    y_score_name: str):
    import pandas as pd
    from rexmex import RatingMetricSet, ScoreCard
        
    metric_set = RatingMetricSet()
    score_card = ScoreCard(metric_set)

    df = pd.read_json(input_data)
    df.rename(columns={y_true_name: 'y_true', y_score_name:'y_score'}, inplace=True)
    
    df = score_card.generate_report(df).reset_index()
    
    return df.to_json(orient='records', lines=False)
