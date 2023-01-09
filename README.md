<h1 align='center'>PySpark</h1>

<h2>About</h2>

<p>This is a small project where i look for more understanding about pyspark framework. In this project i use a public base <i>(link bellow)</i> with it has informations of companies from brazil.</p> 

> [Download base.csv](https://caelum-online-public.s3.amazonaws.com/2273-introducao-spark/01/estabelecimentos.zip)

<p>Base contains aproximally 4.5M rows, this size makes impossible utilize pandas lib even with a good processor and large memory ram.</p>

<p>I maked a really simples treatments in data as replace fill, change columns types and reduce columns utilized.</p>

<p>The fun part was how to do performative batch inserts in PostgreSQL database, 
even with a performance oriented script and good database practices it took an average of 10 minutes to insert 4.5M rows.</p>


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
