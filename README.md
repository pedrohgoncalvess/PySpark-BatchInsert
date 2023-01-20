<h1 align='center'>PySpark</h1>

<h2>About</h2>

<p>This is a small project where i look for more understanding about pyspark framework. In this project i use a public base <i>(link bellow)</i> with it has informations of companies from brazil.</p> 

> [Download base.csv](https://caelum-online-public.s3.amazonaws.com/2273-introducao-spark/01/estabelecimentos.zip)

<p>Base contains aproximally 4.5M rows and 28 columns with a wide range of data types, this size makes impossible utilize pandas lib even with a good processor and large memory ram.</p>

<p>I maked a really simples treatments in data as replace fill, change columns types and reduce columns utilized.</p>

<p>The fun part was how to do performative batch inserts in PostgreSQL database, 
even with a performance oriented script and good database practices it took an average of 10 minutes to insert 4.5M rows with 5 columns.</p>


<p>Although pyspark provides some connections with more performance, I haven't found a better connection between pyspark and postgresql than psycopg2, I've used it to perform the batch insert.

first you will need the package psycopg2 ```pip install psycopg2``` along with pyspark <i>(you can install anyway)</i>, the rest is in the codes</p>


<h2>Archives</h2>

> .env

- Path of .csv archives (10 parts) 
- Login in postgresql database
- Path to directory with spark and hadoop archives for enviroment variables

> Init

<p>In the init file initialize pyspark, load and merge the .csv archives.</p>

> Database

<p>Contains the connection with PostgreSQL.</p>

> Main

<p>Contains the insert batch script</p>


<p><b>Remember, this repository not is a example of good practices programming/pyspark framework. Who made its as a beginner.</b></p>
