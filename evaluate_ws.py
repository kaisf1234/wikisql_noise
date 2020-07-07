#!/usr/bin/env python
import json
from argparse import ArgumentParser
from tqdm import tqdm
from tabulate import tabulate
from errors.error_logger import ErrorLogger
from wikisql.lib.dbengine import DBEngine
from wikisql.lib.query import Query
from wikisql.lib.common import count_lines

import os

# Jan1 2019. Wonseok. Path info has added to original wikisql/evaluation.py
# Only need to add "query" (essentially "sql" in original data) and "table_id" while constructing file.

def oprint(*args, **kwargs):
    #print(*args, **kwargs)
    pass

leeway = 0
if __name__ == '__main__':

    # Hyper parameters
    mode = 'dev'
    ordered = False

    dset_name = 'wikisql_tok'
    saved_epoch = 'best'  # 30-162
    key_data = '/gtlt_noisy_data/'
    key_results = '/sample'
    # Set path
    path_h = './' # change to your home folder
    # path_wikisql_tok = os.path.join(path_h, 'data', 'wikisql_tok')
    path_save_analysis = './results' +  key_results

    # Path for evaluation results.
    path_wikisql0 = os.path.join(path_h,'data/WikiSQL-1.1' + key_data)
    path_source = os.path.join(path_wikisql0, f'{mode}.jsonl')
    path_db = os.path.join(path_wikisql0, f'{mode}.db')
    print(path_db)
    print(os.getcwd())
    path_pred = os.path.join(path_save_analysis, f'results_{mode}.jsonl')


    # For the case when use "argument"
    parser = ArgumentParser()
    parser.add_argument('--source_file', help='source file for the prediction', default=path_source)
    parser.add_argument('--db_file', help='source database for the prediction', default=path_db)
    parser.add_argument('--pred_file', help='predictions by the model', default=path_pred)
    parser.add_argument('--ordered', action='store_true', help='whether the exact match should consider the order of conditions')
    args = parser.parse_args()
    args.ordered=ordered

    all_meta = {}
    fm = open(path_wikisql0 + "/" + mode+".tables.jsonl")
    for line in fm:
        meta = json.loads(line)
        all_meta[meta["id"]] = meta
    fm.close()


    engine = DBEngine(args.db_file)
    exact_match = []


    error_loger = ErrorLogger()
    agg_map = {x : {y: 0 for y in Query.agg_ops} for x in Query.agg_ops}
    with open(args.source_file) as fs, open(args.pred_file) as fp:
        grades = []
        c = 0
        cc = 0
        for ls, lp in tqdm(zip(fs, fp), total=count_lines(args.source_file)):
            eg = json.loads(ls)
            ep = json.loads(lp)
            # In case you want to skip real or fake ones
            if eg.get("is_real", False) == False:
                continue
            # Skip missing entries (Faulty tokenization)
            while eg["question"] != ep["nlu"]:
                ls = next(fs)
                eg = json.loads(ls)
                # print(eg["question"])
                # print(ep["nlu"])
                # print(c)
                grades.append(False)
                exact_match.append(False)
                #dwdwed

            c += 1


            #ep["query"]["agg"] = 3 if ep["query"]["agg"] == 4 else  ep["query"]["agg"] #eg["sql"]["agg"]
            qg = Query.from_dict(eg['sql'], ordered=args.ordered)

            gold = engine.execute_query(eg['table_id'], qg, lower=True)
            pred = ep.get('error', None)
            qp = None
            if not ep.get('error', None):
                try:
                    qp = Query.from_dict(ep['query'], ordered=args.ordered)
                    pred = engine.execute_query(eg['table_id'], qp, lower=True)
                except Exception as e:
                    pred = repr(e)
            correct = pred == gold
            if all_meta[eg["table_id"]]["types"][ep["query"]["sel"]] == "real":
                agg_map[Query.agg_ops[eg["sql"]["agg"]]][Query.agg_ops[ep["query"]["agg"]]] += 1
            if not correct:
                if ep["query"]["agg"] != eg["sql"]["agg"] :
                # if  all_meta[eg["table_id"]]["types"][ep["query"]["sel"]] == "real" and eg["sql"]["agg"] == 3 :
                    cc += 1
                    # print(eg["question"])
                    # print("GOLD", Query.agg_ops[eg["sql"]["agg"]])
                    # print("PRED", Query.agg_ops[ep["query"]["agg"]])
                    # print("GOLD", eg["sql"])
                    # print("PRED", ep["query"])
                    # print(all_meta[eg["table_id"]]["header"])
                    # print(all_meta[eg["table_id"]]["types"])
                    # print(pred, gold)
                    # print("table_" + eg["table_id"].replace("-", "_"))
                    # print("^"*100)
                    # values = engine.execute_sel_star(eg["table_id"])
                    #
                    # print(tabulate(values, headers=all_meta[eg["table_id"]]["header"], tablefmt='fancy_grid'))
                    # print("*"*100)
                    pass
                #oprint("Pred" ,pred)
                #oprint("Gold", gold)
                oprint(all_meta[eg["table_id"]]["header"])
                oprint("Pred", ep["query"])
                oprint("Gold", eg["sql"])
                oprint(eg["table_id"])
                oprint(eg["question"])
                oprint("-"*100)
                pass
            match = qp == qg
            if not match and leeway >= 1:
                leeway -= 1
                match = 1
            grades.append(correct)
            exact_match.append(match)
            error_loger.log(eg, ep, all_meta[eg["table_id"]]["header"])
        print(json.dumps({
            'ex_accuracy': sum(grades) / len(grades),
            'lf_accuracy': sum(exact_match) / len(exact_match),
            }, indent=2))

        print(c)
        print(agg_map)
        error_loger.display()
        error_loger.dump('./errors/' + (key_data+"_"+key_results+"").replace("/", '')+'.log')
        print(cc)

'''
4413
sel :  0.09358712893723091
agg :  0.003399048266485384
col :  0.09494674824382507
op :  0.0018128257421255382
val :  0.058237026965782916
sub_val :  0.024019941083163382
rep_val :  0.03081803761613415
'''

'''
[]
['ensenada, baja california , mexico']
['Wrestler', 'Reign', 'Days held', 'Location', 'Event']
{'agg': 0, 'sel': 3, 'conds': [[1, 0, '2'], [3, 0, 'super parka']]}
{'sel': 3, 'conds': [[0, 0, 'super parka'], [1, 0, '2']], 'agg': 0}
2-14227676-2
Location super parka Wrestler 2 Reign

'''

'''
8419
sel :  0.03563368571089203
agg :  0.09229124599121036
col :  0.06069604466088609
op :  0.016629053331749615
val :  0.0660410975175199
sub_val :  0.02470602209288514
rep_val :  0.029813517044779664
'''