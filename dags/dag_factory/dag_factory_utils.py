def table_columns_format(
    column_list_format,
    force_string = False
):

    formated_columns = " "
    
    for key, value in column_list_format.items():
        if force_string:
            formated_columns = formated_columns + f"{key} varchar" + ","
        else:
            formated_columns = formated_columns + f"{key} {value}" + ","

    return formated_columns[:-1]


def table_columns_select(
    column_list_format,
    number_decimal_format,
    datetime_format,
    apply_cast
):

    if number_decimal_format is None:
        decimal_format = "."
        exclude = ","
    else:
        if number_decimal_format == ".":
            decimal_format = "."
            exclude = ","
        else:
            decimal_format = ","
            exclude = "."

    selected_columns = " "
    for key, value in column_list_format.items():
        if (value[:6] == "number" or value[:5] == "float") and apply_cast == True:
            selected_columns = selected_columns + f"CAST(REPLACE(REPLACE({key},'{decimal_format}',''),'{exclude}','{decimal_format}') as {value}) as {key}" + ","
        elif value in ["datetime", "date", "time"] and apply_cast == True:
            if datetime_format is None:
                selected_columns = selected_columns + f"CAST({key} as {value}) as {key}" + ","
            else:
                selected_columns = selected_columns + f"CAST(TO_TIMESTAMP_NTZ({key},'{datetime_format}') as {value}) as {key}" + ","
        else:
            selected_columns = selected_columns + " " + key + ","

    return selected_columns[:-1]


def create_key_relation(keys):
    filter_keys_list = " "
    for key in keys:
        filter_keys_list += f"AND a.rawdata->>'{key}' = b.rawdata->>'{key}'"
    return filter_keys_list