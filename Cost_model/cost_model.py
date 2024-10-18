import pandas as pd
 
def rule_A(cost: int) -> bool:
    global TOTAL_COST_SUM
    if cost > TOTAL_COST_SUM/4 * ALPHA: return False    
    else: return True



def rule_B(fragments: list) -> bool:
    global TOTAL_VOL_FRAGMENTS
    if len(fragments) > TOTAL_VOL_FRAGMENTS/4 * BETA: return False 
    else: return True
    
    
    
def compute_total_benefit(info: dict):
    '''
    Compute total benefit of current statements.

    Used for computing {rule_A}.
    '''
    total_benefit = 0
    for statement in info:
        row = info[statement]

        if len(row) > ROW_LEN:
            total_benefit += row[BENEFIT]
    
    return total_benefit
    
    
    
def merge(info: dict, producer_key: str, consumer_key: str) -> Tuple[None, None]|Tuple[str, list]:
    '''
    Merge producer and consumer.
    '''
    p = info[producer_key]
    c = info[consumer_key]


    # Compute part of merged info for determination
    cost = p[COST] + c[COST]
    fragments = p[FRAGMENTS] + c[FRAGMENTS]


    # Determine whether to merge using rules.
    if not(rule_A(cost) and rule_B(fragments)): return None, None


    # Compute producer_statements and consumer_statements
    '''
    NOTE: producer/consumer_statements MUST have unique statements.
    c.f. When both producer and consumer have the same statements in producer/consumer_statements list.
    
    This is done by using set().
    '''
    # Remove keys in producer statements
    producer_statements = list(set(p[PRODUCER_STATEMENTS] + c[PRODUCER_STATEMENTS]))

    producer_statements.remove(producer_key)
    if consumer_key in producer_statements:
        producer_statements.remove(consumer_key)

    if producer_statements is None: producer_statements = []

    # Remove keys in consumer statements
    consumer_statements = list(set(p[CONSUMER_STATEMENTS] + c[CONSUMER_STATEMENTS]))

    consumer_statements.remove(consumer_key)
    if producer_key in consumer_statements:
        consumer_statements.remove(producer_key)

    if consumer_statements is None: consumer_statements = []


    # Compute result_size, consist, benefit
    result_size = c[RESULT_SIZE]  #p[3] + c[3]
    consist = []
    benefit = p[RESULT_SIZE]

    # If producer statement is merged one, then inherit it.
    if len(p) > ROW_LEN:  
        result_size += p[RESULT_SIZE]
        consist.extend(p[CONSIST])
        benefit += p[BENEFIT]
    else:
        consist.append(producer_key)
        

    # If consumer statement is merged one, then inherit it.
    if len(c) > ROW_LEN: consist.extend(c[CONSIST])
    else: consist.append(consumer_key)

    consist = sorted(consist)   # for visual


    # Pack the result
    merge_row = [
        producer_statements,
        consumer_statements,
        cost,
        result_size,
        fragments,
        consist,
        benefit
    ]


    # Make new statement
    merge_statement = "EB-" + "-".join(map(str, list(consist)))

    return merge_statement, merge_row