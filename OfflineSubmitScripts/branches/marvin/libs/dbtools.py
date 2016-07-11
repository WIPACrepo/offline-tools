
def max_queue_id(dbs4_, DatasetId):
    """
    Retrieves the max queue id from the `i3filter.job` table.

    Args:
        dbs4_ (SQLClient_dbs4): The SQL client for dbs4.
        DatasetId (int): The dataset id

    Returns:
        int: The max queue id from `i3filter.job`.
    """
    # Get current maximum queue_id for dataset_id, any subsquent submissions starts from queue_id+1
    tmp = dbs4_.fetchall("""select max(queue_id) from i3filter.job where dataset_id=%s """%(DatasetId))
    return tmp[0][0]

