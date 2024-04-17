from sqlparser import parse
from sqlparser.exceptions import *
from caller.caller import func_map

if __name__ == '__main__':
    func_map['connect']()
    while True:
        try:
            sql = input('milvus > ')
        except EOFError:
            break
        except KeyboardInterrupt:
            break
        
        if not sql.endswith(';'):
            sql = sql + ';'
        if sql.lower() == 'exit' or sql.lower() == 'exit;':
            break
        try:    
            query = parse(sql)
            current_db = func_map[query['type']](query)
        except Exception as result:
            print('%s' % result)
    func_map['disconnect']()