#!/usr/bin/env python

from tabulate import tabulate  # Package for pretty-printing tables
import sqlite3  # Package for interfacing with sqlite3 databases
import os.path


####################
# SETUP            #
####################
def SetupInMemory(DB_PATH):
    # Make sure that the database file exists
    if not os.path.isfile(DB_PATH):
        print("Could not find database file at: {}".format(DB_PATH))
        exit(1)

    # Establish a connection to the database
    source_connection = sqlite3.connect(DB_PATH)

    # Establish a connection to an in-memory database
    dest_connection = sqlite3.connect(":memory:")

    # Copy the data from the source DB to the destination
    source_connection.backup(dest_connection)

    # Cleanly close the source connection
    source_connection.close()

    # Get a 'cursor' object for querying the database
    cursor = dest_connection.cursor()

    return dest_connection, cursor


####################
# TEARDOWN         #
####################
def Teardown(db_connection, cursor):
    # Close the cursor object
    cursor.close()

    # Clost the connection to the database
    db_connection.close()


#####################
# CHECKPOINT HELPER #
#####################
def CheckpointHelper(K, results, checkpoints_on, is_candidate):
    # Check each iteration of the candidate generation
    if is_candidate and checkpoints_on:
        expected = []
        if K == 1:
            expected = [[1, 2], [1, 3], [1, 4], [1, 5], [2, 3], [2, 4], [2, 5], [3, 4], [3, 5], [4, 5]]
        if K == 2:
            expected = [[1, 2, 3], [1, 2, 5]]
        error_message = "Unexpected number of candidates at level {}: {}".format(K, len(results))
        assert len(results) == len(expected), error_message
        error_message = "Unexpected candidate result at level {}: {}".format(K, results)
        assert results == expected, error_message

    # Check each iteration of frequent itemsets
    elif not is_candidate and checkpoints_on:
        expected = []
        if K == 1:
            expected = [[1, 2], [1, 3], [1, 5], [2, 3], [2, 4], [2, 5]]
        if K == 2:
            expected = [[1, 2, 3], [1, 2, 5]]
        error_message = "Unexpected number of frequent itemsets at level {}: {}".format(K, len(results))
        assert len(results) == len(expected), error_message
        error_message = "Unexpected frequent itemsets at level {}: {}".format(K, results)
        assert results == expected, error_message


####################
# GET TRANSACTIONS #
####################
def GetTransactions(cursor):
    # List of transactions to return
    transactions = []

    # TODO: Define query
    QUERY = "SELECT order_id,product_id FROM OrderProducts;"

    # TODO: Execute query
    query_exe = cursor.execute(QUERY)

    # TODO: Fetch all rows (as a list) that result from executing the query
    results = list(cursor.fetchall())
    print(results)

    # TODO: Populate the transactions list with the transaction data
    lists = []
    temp = results[0][0]
    for x in range(len(results)):
        if results[x][0] == temp:
            lists.append(results[x][1])
            if x == len(results) - 1:
                transactions.append(lists)
        else:
            transactions.append(lists)
            lists = [results[x][1]]
            temp = results[x][0]

    print(transactions)

    # transactions.insert(x, result[x])

    '''
    Note: You may store the transaction data in whatever format you would like.
    For my implementation, I chose to create a list of integers for each transaction, 
    representing all the product_ids associated with a given order.
    '''

    return transactions


###########################
# GET FREQUENT 1-ITEMSETS #
###########################
def GetFrequent1Itemsets(cursor, min_support_abs):
    # List of 1-itemsets to return
    itemsets = []

    # TODO: Define query
    QUERY = "SELECT product_id, COUNT(product_id) FROM OrderProducts GROUP BY product_id;"

    # TODO: Execute query
    query_exe = cursor.execute(QUERY)

    # TODO: Fetch all rows that result from executing the query
    results = list(cursor.fetchall())

    # TODO: Populate the itemsets list with the frequent 1-itemset data

    for x in range(len(results)):
        if results[x][1] >= min_support_abs:
            itemsets.append([results[x][0]])

    '''
    Note: You may store the 1-itemset data in whatever format you would like.
    For my implementation, I chose to store each one itemset as a list of size 1, 
    containing a single product_id.
    '''

    return itemsets


####################
# GET CANDIDATES   #
####################
def GetCandidates(cursor, transactions, L_K):
    # The list of candidates to return
    C = []

    # TODO: Self-join L_K with L_K
    #  - TODO: Check that the first K items match
    #  - TODO: Check that the K+1 element on the LHS < RHS
    # QUERY = "SELECT * FROM OrderProducts AS a JOIN OrderProducts AS b ON a.product_id = b.product_id"
    # query_exe = cursor.execute(QUERY)
    # results = list(cursor.fetchall())

    if isinstance(L_K[0], int):
        k = len(L_K)
        for i in range(k):
            for j in range(i + 1, k):
                if L_K[i] < L_K[j]:
                    C.append([L_K[i], L_K[j]])
    else:
        temp = []
        k = len(L_K)
        for i in range(k):
            for j in range(i + 1, k):
                results = []
                for x in range(len(L_K[i])):
                    if x == len(L_K[i]) - 1:
                        if L_K[i][x] < L_K[j][x]:
                            results.append(L_K[i][x])
                            results.append(L_K[j][x])
                            temp.append(results)

                    elif L_K[i][x] == L_K[j][x]:
                        results.append(L_K[i][x])
                    else:
                        break
        for a in range(len(temp)):
            C.append(temp[a])
        for x in range(len(temp)):
            y = subsets_of_given_size(temp[x], len(temp[x])-1)
            for z in y:
                if z not in L_K:
                    C.remove(temp[x])
                    break


        # for x in temp:
        #     for y in transactions:
        #         if set(x)  y:
        #             C.append(x)

    # TODO: Prune all candidates that have non-frequent subsets
    return C


def subsets_of_given_size(list1, n):
    return [x for x in subsets(list1) if len(x) == n]


def subsets(list1):
    if list1 == []:
        return [[]]
    x = subsets(list1[1:])
    return x + [[list1[0]] + y for y in x]


####################
# CHECK SUPPORT    #
####################
def CheckSupport(cursor, transactions, C, min_support_count):
    # The list of frequent k-itemsets to return (support > the minimum support threshold)
    F = []

    # TODO: Find the support for each candidate and check against the minimum support threshold
    for i in C:
        m = 0
        for j in transactions:
            if set(i) & set(j) == set(i):
                m = m + 1
        if m >= min_support_count:
            F.append(i)

    return F


####################
# GET CONFIDENCE   #
####################
def GetConfidence(cursor, transactions, lhs, rhs):
    assert isinstance(lhs, list), "Parameter is expected to be a list"
    assert isinstance(rhs, list), "Parameter is expected to be a list"

    # The confidence to return for the association rule: lhs -> rhs
    confidence = None

    # TODO: calculate the confidence
    m = 0
    n = 0
    for i in range(len(transactions)):
        if set(transactions[i]).intersection(set(lhs).union(rhs)) == set(lhs).union(rhs):
            m = m+1
        if set(transactions[i]).intersection(set(lhs)) == set(lhs):
            n = n+1
    confidence = m/n


    return confidence


####################
# CHECK CONFIDENCE #
####################
def CheckConfidence(cursor, transactions, F, min_confidence_percentage):
    # The strong association rules to return (confidence > the minimum confidence threshold)
    A = []
    for itemset in F:
        # Itemsets of size 1 cannot be made into an association rule
        if len(itemset) <= 1:
            continue
        for item in itemset:
            other_items = itemset.copy()
            other_items.remove(item)

            # Check rule for: item -> [other_items]
            confidence = GetConfidence(cursor, transactions, [item], other_items)
            if confidence >= min_confidence_percentage:
                A.append([item, other_items, confidence])

            # Check rule for: [other_items] -> item                
            confidence = GetConfidence(cursor, transactions, other_items, [item])
            if confidence >= min_confidence_percentage:
                A.append([other_items, item, confidence])
    return A


####################
# APRIORI          #
####################
def Apriori(db_path, min_support_percentage, min_conf_percentage, checkpoints_on=False):
    # TODO: Create an in-memory database connection
    db_connection, cursor = SetupInMemory(db_path)

    # TODO: Get all transactions
    transactions = GetTransactions(cursor)
    if checkpoints_on:
        error_message = "Unexpected number of transactions: {}".format(len(transactions))
        assert len(transactions) == 9

    # TODO: Convert the minimum support threshold from a percentage to an absolute value
    min_support_count = abs(min_support_percentage) * len(transactions)
    if checkpoints_on:
        error_message = "Unexpected minimum support count: {}".format(min_support_count)
        assert min_support_count == 2, error_message

    # TODO: Get frequent 1-itemsets
    L1 = GetFrequent1Itemsets(cursor, min_support_count)
    if checkpoints_on:
        assert len(L1) == 5, "Unexpected number of frequent 1-itemsets: {}".format(len(L1))

    # Start with k=1 since we already found the 1-itemsets
    K = 1

    # Stores the frequent itemsets at each level
    L = {}
    L[1] = L1  # Frequent itemsets at level 1

    # TODO: Iterate until there are no new frequent k-itemsets
    while L[K]:
        # TODO: Get candidates
        C = GetCandidates(cursor, transactions, L[K])
        CheckpointHelper(K, C, checkpoints_on, is_candidate=True)

        # TODO: Check againast the minimum support threshold
        F = CheckSupport(cursor, transactions, C, min_support_count)
        CheckpointHelper(K, F, checkpoints_on, is_candidate=False)

        # TODO: Increment K
        K = K + 1

        # TODO: Save off frequent itemsets at level K
        L[K] = F

    # TODO: Flatten the frequent itemsets from each level into a single list
    frequent_itemsets = []
    for item in range(1,len(L)):
        for x in L[item]:
            frequent_itemsets.append(x)


    if checkpoints_on:
        # Validate the results against expected values
        expected = [[1], [2], [3], [4], [5], [1, 2], [1, 3], [1, 5], [2, 3], [2, 4], [2, 5], [1, 2, 3], [1, 2, 5]]
        error_message = "Unexpected number of results: {}".format(len(frequent_itemsets))
        assert len(frequent_itemsets) == len(expected), error_message
        error_message = "Unexpected frequent itemsets: {}".format(frequent_itemsets)
        assert frequent_itemsets == expected, error_message

    # TODO: Find strong association rules from the frequent itemsets using the minimum confidence threshold
    association_rules = CheckConfidence(cursor,transactions,frequent_itemsets,min_conf_percentage)
    if checkpoints_on:
        error_message = "Unexpected number of association rules: {}".format(len(association_rules))
        assert len(association_rules) == 9, error_message

    # TODO: Teardown the connection
    Teardown(db_connection,cursor)

    # Return the found strong association rules
    return association_rules


####################
# MAIN             #
####################
if __name__ == "__main__":

    # Run the apriori algorithm on the textbook example
    min_support = (2.0 / 9.0)
    min_confidence = 0.75
    results = Apriori("./all_electronics.db", min_support, min_confidence, checkpoints_on=True)

    # Write results to file
    f = open("all_electronics.txt", "w")
    for result in results:
        lhs, rhs, conf = result
        f.write("{} -> {} : {} \n".format(lhs, rhs, conf))
    f.close()

    # Optional: Try your implementation on our grocery database
    RUN_GROCERY_DB = False
    if RUN_GROCERY_DB:
        # Run the apriori algorithm on the grocery data
        results = Apriori("./grocery.db", 0.01,0.75, checkpoints_on=False)

        # Write results to file
        f = open("grocery.txt", "w")
        f.write(str(results))
        f.close()
