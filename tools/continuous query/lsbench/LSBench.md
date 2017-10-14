# LSBench Continuous Queries

## LSBench Query1
```sql
REGISTER QUERY query1_single_user_actions AS

SELECT ?p ?o
FROM poststream
[ RANGE 1s STEP 100ms ]
FROM gpsstream
[ RANGE 1s STEP 100ms ]

WHERE
{
  sibu:u6496 ?p ?o .
}
```

## LSBench Query2
```sql
REGISTER QUERY query2_group_post AS

SELECT ?post
FROM poststream
[ RANGE 1s STEP 100ms ]

WHERE
{
  ?user sioc:account_of sibp:p864 .
  ?user sioc:creator_of ?post .
}
```

## LSBench Query3
```sql
REGISTER QUERY query3_group_friend_post AS

SELECT ?friend ?post
FROM poststream
[ RANGE 1s STEP 100ms ]

WHERE
{
  ?user sioc:account_of sibp:p3514 .
  ?user foaf:knows ?friend .
  ?friend sioc:creator_of ?post .
}
```

## LSBench Query4
```sql
REGISTER QUERY query4_posts_same_tag AS

SELECT ?post1 ?post2 ?tag
FROM poststream
[ RANGE 1s STEP 100ms ]

WHERE
{
  ?post1 sib:hashtag ?tag .
  ?user sioc:creator_of ?post1 .
  ?user sioc:creator_of ?post2 .
  ?post2 sib:hashtag ?tag .
  FILTER (?post1 != ?post2)
}
```

## LSBench Query5
```sql
REGISTER QUERY query5_photo_friend_like AS

SELECT ?friend1 ?friend2 ?photo
FROM photostream
[ RANGE 1s STEP 100ms ]
FROM photolikestream
[ RANGE 1s STEP 100ms ]

WHERE
{
  ?photo sib:usertag ?friend1 .
  ?friend1 foaf:knows ?friend2 .
  ?friend2 sib:like ?photo .
}
```

## LSBench Query6
```sql
REGISTER QUERY query6_post_complex AS

SELECT ?user ?friend ?post ?channel
FROM poststream
[ RANGE 1s STEP 100ms ]
FROM postlikestream
[ RANGE 1s STEP 100ms ]

WHERE
{
  ?channel sioc:container_of ?post .
  ?user sioc:subscriber_of ?channel .
  ?user foaf:knows ?friend .
  ?friend sib:like ?post .
}
```