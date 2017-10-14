# LSBench One-shot Queries

## LSBench One-shot Query1
```sql
SELECT ?p ?o  
WHERE 
{
    sibu:u984 ?p ?o .
}
```

## LSBench One-shot Query2
```sql
SELECT ?post 
WHERE 
{
    ?user sioc:account_of sibp:p781 .
    ?user sioc:creator_of ?post .
}
```

## LSBench One-shot Query3
```sql
SELECT ?friend ?post  
WHERE 
{
    ?user sioc:account_of sibp:p984 .
    ?user foaf:knows ?friend .  
    ?friend sioc:creator_of ?post .
}
```

## LSBench One-shot Query4
```sql
SELECT ?user1 ?photo ?album ?user2  
WHERE 
{
    sibg:g1  sioc:has_member  ?user1 .
    ?photo  sib:usertag  ?user1 .
    ?album  sioc:container_of  ?photo .
    ?user2  sioc:creator_of  ?album .
}
```

## LSBench One-shot Query5
```sql
SELECT ?user ?profile ?gender ?day ?email  
WHERE 
{
    sibu:u984  foaf:knows  ?user .
    ?user  sioc:account_of  ?profile .
    ?profile  foaf:gender ?gender .
    ?profile  foaf:birthday  ?day . 
    ?profile  sioc:email  ?email .
}
```

## LSBench One-shot Query6
```sql
SELECT ?user1 ?post ?user2 ?tag
WHERE 
{
    ?user1  foaf:based_near  dbp:Peoples_Republic_of_China .
    ?user1  sioc:creator_of  ?post .
    ?user2  sib:like  ?post .
    ?user2  foaf:based_near  dbp:United_States .
    ?post  sib:hashtag  ?tag .
}
```