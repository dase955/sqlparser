# Operator

下面是Milvus里需要支持的Operator

 - 'and', 'or': 已经实现

 - '+', '-', '*', '/', ' ** ', '%': 这些在SQL直接对应 '+', '-', '*', '/', ' ** ', '%'即可。'**' 可以看为'*' '*'

 - '<', '>', '==', '!=', '<=', '>=': 已经实现

 - 'not', 'like', 'in': 已经实现

 - JSON_CONTAINS: json = {"x": [[1,2,3], [4,5,6], [7,8,9]]}, json_contains(x, [1,2,3]) -> true

 - JSON_CONTAINS_ALL: json = {"x": [1,2,3,4,5,7,8]}, json_contains_all(x, [1,2,8]) -> true

 - JSON_CONTAINS_ANY: json = {"x": [1,2,3,4,5,7,8]}, json_contains_any(x, [6,9]) -> false

 - ARRAY_CONTAINS: array = [1,2,3], array_contains(array, 1) -> true

 - ARRAY_CONTAINS_ALL: array = [1,2,3], array_contains_all(array, [1,5]) -> false

 - ARRAY_CONTAINS_ANY: array = [1,2,3], array_contains_any(array, [1,5]) -> true

 - ARRAY_LENGTH: array = [1,2,3], array_length(array) -> 3