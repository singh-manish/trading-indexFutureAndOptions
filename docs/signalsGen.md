# Introduction

These set of programs form essential part of automated trading system being developed. At high level, whole system consists of three interlinked but independent part.
First part is signal generation part. This part would keep on generating possible entry signals at frequent intervals.
Second part is trade execution part. This part monitors all the entry signals being generated and enters only those trades which meet all entry criteria such as limit on existing open positions, 
limit on number of trades already executed during the day, limit on margins required, current time, in case of existing position for same symbol - whether duplicate positions are allowed or not ....
Second part also monitors the existing open positions for stop loss limits, take profit limits and takes actions as per rules configured (trailing stop loss, maximum holding period etc.).
Second part also monitors manual intervention messages if one wants to exit a trade maually (overriding other configured rules for automatic execution).
Third part is monitoring, reporting and interface for intervention. This part monitors the trades, open positions and generates reports. This part also generates entries which causes trade execution 
behaviour to be altered (e.g. number of simultneous positions to be held can be changes, limit on daily stop loss OR take profit could be altered, one/all open positions could be squared off at market etc..)
 
## Evolution

These programs have evolved over time, especially first part (entry signal generation part). Based on findings, patterns in data, discovery of better models etc.., it would keep evolving further. 
Ultimate target is to have a fully automated system which can give greater than 25% net return on consistenet basis with holding period not more than 3 trading days and 3x margin.

## First part - Entry Signal Generation

Covered in detail under signalGen.md.

## Second part - Trade Execution

Covered in detail under tradesExec.md.

## Third part - Reporting and Monitoring

Covered in detail under reportMonitoring.md.

## Queries / Feedback

Please send queries OR feedback to Manish Singh: mksingh at hotmail dotcom