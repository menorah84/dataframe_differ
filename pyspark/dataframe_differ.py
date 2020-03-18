"""
Example use:
pk = 'primary_key'
a = spark.read.csv(path='.csv', header=True)
b = spark.read.csv(path='.csv', header=True)

results_diff = get_diff(a, b, pk)
"""

# Parameters
# a:    pyspark DataFrame
# b:    pyspark DataFrame
# pk:   column name of the key
# returns has_difference (bool), difference (dict)
def get_diff(a, b, pk):

    # 1. Check first if schemas are the same
    # TODO: Check also difference in schema data types
    # current assumption is same column name has same type
    if df1.schema.names != df2.schema.names:
        return True, { "message" : "Schemas do not match" }

    # 2. Check if same number of rows
    # May not be useful after all, as we are inspecting per row

    # 3. (Symmetric difference) Eliminate intersection of rows from each side: if nothing remains on both side, it means they are equal
    a_minus_b = a.subtract(a.intersect(b))
    b_minus_a = b.subtract(b.intersect(a))

    if len(a_minus_b.take(1)) == 0 and len(b_minus_a.take(1)) == 0:
        return False, { "message": "Both are equal" }

    # 4. From the symmetric difference, find the rows that has the same keys on both side: it means that some column values for these same-key rows do not match
    result_diff = { "a_not_in_b": [], "b_not_in_a": [], "same_key_but_diff_values": [] }

    column_names = df1.schema.names

    a_pks = a_minus_b.select(pk).rdd.flatMap(lambda x: x).collect()
    b_pks = b_minus_a.select(pk).rdd.flatMap(lambda x: x).collect()
    same_key_diff_val = list(set(a_pks) & set(b_pks))

    result_diff['same_key_but_diff_values'] = get_diff_same_keys(a_minus_b, b_minus_a, column_names, pk, same_key_diff_val)
    result_diff['same_key_but_diff_values'] = sorted(result_diff['same_key_but_diff_values'], key = lambda x: x[pk])

    # 5. List the symmetric difference with the exception from step 4
    if len(a_minus_b.take(1)) > 0:
        a_minus_b_2 = a_minus_b.subtract(a_minus_b[a_minus_b[pk].isin(same_key_diff_val)])
        result_diff['a_not_in_b'] = list(a_minus_b_2.select(pk).toPandas()[pk])
        result_diff['a_not_in_b'].sort()

    if len(b_minus_a.take(1)) > 0:
        b_minus_a_2 = b_minus_a.subtract(b_minus_a[b_minus_a[pk].isin(same_key_diff_val)])
        result_diff['b_not_in_a'] = list(b_minus_a_2.select(pk).toPandas()[pk])
        result_diff['b_not_in_a'].sort()

    return True, { "message": "Mismatch on some rows.", "difference": result_diff }


# Utility function
def get_diff_same_keys(df1, df2, column_names, pk, same_keys):
    diff_cols = []

    for id in same_keys:
        row1 = df1.filter(df1[pk] == id).first()
        row2 = df2.filter(df2[pk] == id).first()
        cols = []

        for col in column_names:
            if row1[col] != row2[col]:
                cols.append( {col: { "a": row1[col], "b": row2[col] } } )

        diff_cols.append( {pk: id, "columns": cols} )

    return diff_cols
