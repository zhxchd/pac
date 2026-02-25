SELECT cntrycode,
       pac_count(pac_hash) AS numcust,
       pac_sum(pac_hash, c_acctbal) AS totacctbal
FROM (
    SELECT substring(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal,
    pac_select_gt(
        hash(c_custkey),
        c_acctbal, (
            SELECT pac_avg_counters(hash(c_custkey), c_acctbal)
            FROM customer
            WHERE c_acctbal > 0.00
            AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17'))) AS pac_hash,
    FROM customer
    WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND pac_filter(pac_hash)
    AND NOT EXISTS (
        SELECT 1
        FROM orders WHERE o_custkey = customer.c_custkey)) AS custsale
GROUP BY cntrycode
ORDER BY cntrycode;
