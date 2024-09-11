SELECT idTransaction     AS idTransacao,
       idCustomer        AS idCliente,
       dtTransaction     AS dtTransacao,
       pointsTransaction AS nrPontosTransacao
FROM 
       bronze.saude_dados.transactions