-- Databricks notebook source
DECLARE variable text string;
declare variable runtime decimal(5,1) default 14.1;
set var text="I love SQL and Databrics"
select concat(text,'<3') as dbk,concat(runtime,'Beta') as Databricks_version;
