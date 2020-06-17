import json


class ErrorLogger():
    def __init__(self):
        self.all_errors = []

    def log(self, gold, pred, headers):
        gold_query = gold["sql"]
        pred_query = pred["query"]
        errors = {'sel' : 0, 'agg' : 0, 'col' : 0, 'op' : 0, 'val' : 0, 'sub_val' : 0, 'rep_val' : 0}
        errors['sel'] = gold_query["sel"] != pred_query["sel"]
        errors['agg'] = gold_query["agg"] != pred_query["agg"]

        gold_conds = {cond[0] : cond for cond in gold_query["conds"]}
        pred_conds = {cond[0] : cond for cond in pred_query["conds"]}
        for col in gold_conds:
            gold_cond = gold_conds[col]
            pred_cond = pred_conds.get(col, None)
            if not pred_cond:
                errors['col'] += 1
                continue
            pred_cond[2] = pred_cond[2].lower() if type(pred_cond[2]) == type(' ') else pred_cond[2]
            gold_cond[2] = gold_cond[2].lower() if type(gold_cond[2]) == type(' ') else gold_cond[2]
            pred_cond[2] = self.__maybe_make_int(pred_cond[2])
            gold_cond[2] = self.__maybe_make_int(gold_cond[2])
            if pred_cond[1] != gold_cond[1]:
                errors['op'] += 1
            elif pred_cond[2] != gold_cond[2]:
                errors['val'] += 1
                if (type(pred_cond[2]) == type(' ') and type(gold_cond[2]) == type(' ')) and (pred_cond[2] in gold_cond[2] or gold_cond[2] in pred_cond[2]):
                    errors['sub_val'] += 1
        pred_values = [x[2] for x in pred_query["conds"]]
        if len(set(pred_values)) != len(pred_values):
            errors['rep_val'] = - len(set(pred_values)) + len(pred_values)
        errors['num_cols'] = len(headers)
        errors['table_id'] = gold['table_id']
        errors['avg_cols'] = sum([len(x.split()) for x in headers])/len(headers)
        self.all_errors.append(errors)

    def display(self):
        col_to_avg = ['sel', 'agg', 'col', 'op', 'val', 'sub_val', 'rep_val']
        for col in col_to_avg:
            print(col, ": ", self.__avg_col(col))


    def __avg_col(self, col):
        vals = [x[col] for x in self.all_errors]
        total = sum(vals)
        avg = total/len(vals)
        return avg

    def dump(self, fname):
        with open(fname, 'w') as f:
            json.dump(self.all_errors, f)

    def __maybe_make_int(self, param):
        x = param
        try:
            x = int(param)
        except:
            pass
        return x




