# CityBench Queries

## CityBench Query1
```sql
REGISTER QUERY citybench_query1 AS
SELECT ?obId1 ?obId2 ?v1 ?v2
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData182955> [range 3000ms step 1s]
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505> [range 3000ms step 1s]
 FROM <http://127.0.0.1:9000/WebGlCity/RDF/SensorRepository.rdf> 
 
WHERE {

?p1   a <http://www.insight-centre.org/citytraffic#CongestionLevel>.
?p2   a <http://www.insight-centre.org/citytraffic#CongestionLevel>.
 
{
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p1.
?obId1 <http://purl.oclc.org/NET/sao/hasValue> ?v1.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData182955>.
}

{
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p2.
?obId2 <http://purl.oclc.org/NET/sao/hasValue> ?v2.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505>.
}

}

```

## CityBench Query2
```sql
REGISTER QUERY citybench_query2 AS
SELECT ?obId1 ?obId2 ?obId3 ?obId4 ?v1 ?v2 ?v3 ?v4
 FROM <http://127.0.0.1:9000/WebGlCity/RDF/SensorRepository.rdf> 
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusWeatherData0> [range 3s step 1s] 
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505> [range 3s step 1s]
 
WHERE {

?p1   a <http://www.insight-centre.org/citytraffic#Temperature>.
?p2   a <http://www.insight-centre.org/citytraffic#Humidity>.
?p3   a <http://www.insight-centre.org/citytraffic#WindSpeed>.
?p4   a <http://www.insight-centre.org/citytraffic#CongestionLevel>.

{
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p1.
?obId1 <http://purl.oclc.org/NET/sao/hasValue> ?v1.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusWeatherData0>.


?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p2.
?obId2 <http://purl.oclc.org/NET/sao/hasValue> ?v2.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusWeatherData0>.


?obId3 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p3.
?obId3 <http://purl.oclc.org/NET/sao/hasValue> ?v3.
?obId3 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusWeatherData0>.
}

{
?obId4 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p4.
?obId4 <http://purl.oclc.org/NET/sao/hasValue> ?v4.
?obId4 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505>.
}

}
```

## CityBench Query3
```sql
REGISTER QUERY citybench_query3 AS
SELECT ?obId1  ?obId3  ?v1  ?v3   (((?v1+?v3)/2) as ?avgCongest)
 FROM <http://127.0.0.1:9000/WebGlCity/RDF/SensorRepository.rdf>  
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData182955> [range 3s step 1s] 
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505> [range 3s step 1s] 

WHERE {
{?p1   a <http://www.insight-centre.org/citytraffic#CongestionLevel>.} 
{?p3   a <http://www.insight-centre.org/citytraffic#CongestionLevel>. }

{
?obId1 a ?ob.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p1.
?obId1 <http://purl.oclc.org/NET/sao/hasValue> ?v1.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData182955>
}

{
?obId3 a ?ob.
?obId3 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p3.
?obId3 <http://purl.oclc.org/NET/sao/hasValue> ?v3.
?obId3 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505>.
}

}
```

## CityBench Query4
```sql
REGISTER QUERY citybench_query4 AS
SELECT ?evtId ?title ?node ?obId2 ?lat2 ?lon2 ?lat1 ?lon1
 FROM <http://127.0.0.1:9000/WebGlCity/RDF/AarhusCulturalEvents.rdf>  
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#UserLocationService> [range 3s step 1s] 

WHERE {
?evtId a  <http://purl.oclc.org/NET/sao/Point>. 
?evtId <http://purl.oclc.org/NET/ssnx/ssn#featureOfInterest> ?foi . 
?foi <http://www.insight-centre.org/citytraffic#hasFirstNode> ?node . 
?node <http://www.insight-centre.org/citytraffic#hasLatitude> ?lat1 .  
?node <http://www.insight-centre.org/citytraffic#hasLongitude> ?lon1 . 
?evtId <http://purl.oclc.org/NET/sao/value> ?title. 

?obId2 a <http://purl.oclc.org/NET/ssnx/ssn#Observation>. 
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p2. 
?obId2 <http://purl.oclc.org/NET/sao/hasValue> ?v2. 
?v2 <http://www.insight-centre.org/citytraffic#hasLatitude> ?lat2. 
?v2 <http://www.insight-centre.org/citytraffic#hasLongitude> ?lon2. 
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#UserLocationService>.

Filter (((?lat2 - ?lat1)*(?lat2 - ?lat1) + (?lon2 - ?lon1)*(?lon2 - ?lon1)) < 0.1)
} 

```

## CityBench Query5
```sql
REGISTER QUERY citybench_query5 AS
SELECT  ?evtId ?title #?lat1 ?lon1 ?obId2  ?lat2 ?lon2
 FROM <http://127.0.0.1:9000/WebGlCity/RDF/AarhusCulturalEvents.rdf>  
 FROM <http://127.0.0.1:9000/WebGlCity/RDF/SensorRepository.rdf>  
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505> [range 3s step 1s]

WHERE {
?p2   a <http://www.insight-centre.org/citytraffic#CongestionLevel>. 
?p2   <http://purl.oclc.org/NET/ssnx/ssn#isPropertyOf> ?foi2.
?foi2 <http://www.insight-centre.org/citytraffic#hasStartLatitude> ?lat2. 
?foi2 <http://www.insight-centre.org/citytraffic#hasStartLongitude> ?lon2. 
 
 
{ 
?evtId a ?ob.
?evtId <http://purl.oclc.org/NET/ssnx/ssn#featureOfInterest> ?foi. 
?foi <http://www.insight-centre.org/citytraffic#hasFirstNode> ?node. 
?node <http://www.insight-centre.org/citytraffic#hasLatitude> ?lat1.  
?node <http://www.insight-centre.org/citytraffic#hasLongitude> ?lon1. 
?evtId <http://purl.oclc.org/NET/sao/value> ?title.
}

 
{
?obId2 a ?ob.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p2.
?obId2 <http://purl.oclc.org/NET/sao/hasValue> ?v2.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505>.
}

Filter (((?lat2-?lat1)*(?lat2-?lat1)+(?lon2-?lon1)*(?lon2-?lon1)) < 0.1)
}
```

## CityBench Query6
```sql
REGISTER QUERY citybench_query6 AS
SELECT ?obId1 ?obId2 ?lat1 ?lon1 ?lat2 ?lon2
 FROM <http://127.0.0.1:9000/WebGlCity/RDF/SensorRepository.rdf>  
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataKALKVAERKSVEJ> [range 3s step 1s] 
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#UserLocationService> [range 3s step 1s] 

WHERE {
?p1   a <http://www.insight-centre.org/citytraffic#ParkingVacancy>.
?p1 <http://purl.oclc.org/NET/ssnx/ssn#isPropertyOf> ?foi1.
?foi1 <http://www.insight-centre.org/citytraffic#hasStartLatitude> ?lat1.
?foi1 <http://www.insight-centre.org/citytraffic#hasStartLongitude> ?lon1.


{
?obId1 a ?ob.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p1.
?obId1 <http://purl.oclc.org/NET/sao/hasValue> ?v1.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataKALKVAERKSVEJ>.
}

{
?obId2 a ?ob.
?obId2 <http://purl.oclc.org/NET/sao/hasValue> ?v2.
?v2 <http://www.insight-centre.org/citytraffic#hasLatitude> ?lat2.
?v2 <http://www.insight-centre.org/citytraffic#hasLongitude> ?lon2.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#UserLocationService>.
}

}

```

## CityBench Query7
```sql
REGISTER QUERY citybench_query7 AS
SELECT ?obId1 ?obId2 ?v1 ?v2 
 FROM <http://127.0.0.1:9000/WebGlCity/RDF/SensorRepository.rdf>  
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataKALKVAERKSVEJ> [range 3s step 1s] 
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataSKOLEBAKKEN> [range 3s step 1s] 

WHERE {
?p1   a <http://www.insight-centre.org/citytraffic#ParkingVacancy>.
?p2   a <http://www.insight-centre.org/citytraffic#ParkingVacancy>.

{
?obId1 a ?ob.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p1.
?obId1 <http://purl.oclc.org/NET/sao/hasValue> ?v1.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataKALKVAERKSVEJ>.
}

{
?obId2 a ?ob.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p2.
?obId2 <http://purl.oclc.org/NET/sao/hasValue> ?v2.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataSKOLEBAKKEN>.
}

Filter(?v1 < 1 || ?v2 < 1 )
}

```

## CityBench Query8
```sql
REGISTER QUERY citybench_query8 AS
SELECT ?obId1 ?obId2 ?v1 ?v2 
 FROM <http://127.0.0.1:9000/WebGlCity/RDF/SensorRepository.rdf>  
 FROM <http://127.0.0.1:9000/WebGlCity/RDF/AarhusLibraryEvents.rdf>  
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataKALKVAERKSVEJ> [range 3s step 1s] 
 FROM  stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataSKOLEBAKKEN> [range 3s step 1s] 

WHERE {
?p1   a <http://www.insight-centre.org/citytraffic#ParkingVacancy>.
?p2   a <http://www.insight-centre.org/citytraffic#ParkingVacancy>.

{
?evtId <http://purl.oclc.org/NET/ssnx/ssn#featureOfInterest> ?foi. 
?foi <http://www.insight-centre.org/citytraffic#hasFirstNode> ?node. 
?node <http://www.insight-centre.org/citytraffic#hasLatitude> ?lat1.  
?node <http://www.insight-centre.org/citytraffic#hasLongitude> ?lon1. 
?evtId <http://purl.oclc.org/NET/sao/value> ?title.
}


{
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p1.
?obId1 <http://purl.oclc.org/NET/sao/hasValue> ?v1.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataKALKVAERKSVEJ>.
}

{
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p2.
?obId2 <http://purl.oclc.org/NET/sao/hasValue> ?v2.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataSKOLEBAKKEN>.
}

Filter(?v1 > 0 || ?v2 > 0)
}
```

## CityBench Query9
```sql
REGISTER QUERY citybench_query9 AS
SELECT ?obId1 ?obId2 ?v1 ?v2 
 FROM <http://127.0.0.1:9000/WebGlCity/RDF/AarhusCulturalEvents.rdf>  
 FROM <http://127.0.0.1:9000/WebGlCity/RDF/SensorRepository.rdf>  
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataKALKVAERKSVEJ> [range 3s step 1s] 
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataSKOLEBAKKEN> [range 3s step 1s] 

WHERE {
?p1   a <http://www.insight-centre.org/citytraffic#ParkingVacancy>.
?p2   a <http://www.insight-centre.org/citytraffic#ParkingVacancy>.

{
?evtId a ?ob.
?evtId <http://purl.oclc.org/NET/ssnx/ssn#featureOfInterest> ?foi. 
?foi <http://www.insight-centre.org/citytraffic#hasFirstNode> ?node. 
?node <http://www.insight-centre.org/citytraffic#hasLatitude> ?lat1.  
?node <http://www.insight-centre.org/citytraffic#hasLongitude> ?lon1. 
?evtId <http://purl.oclc.org/NET/sao/value> ?title.
}


{?obId1 a ?ob.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p1.
?obId1 <http://purl.oclc.org/NET/sao/hasValue> ?v1.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataKALKVAERKSVEJ>.
}
 
{?obId2 a ?ob.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p2.
?obId2 <http://purl.oclc.org/NET/sao/hasValue> ?v2.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataSKOLEBAKKEN>.
}

}

```

## CityBench Query10
```sql
REGISTER QUERY citybench_query10 AS
SELECT ?obId1 ?obId2 #?lat1 ?lon1 ?lat2 ?lon2 ?v1 ?v2 ((?v1+?v2) as ?sumOfAPI)
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusPollutionData201399> [range 3s step 1s] 
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusPollutionData184892> [range 3s step 1s] 
 FROM <http://127.0.0.1:9000/WebGlCity/RDF/SensorRepository.rdf> 

WHERE {

?p1 <http://purl.oclc.org/NET/ssnx/ssn#isPropertyOf> ?foi1.
?foi1 <http://www.insight-centre.org/citytraffic#hasStartLatitude> ?lat1.
?foi1 <http://www.insight-centre.org/citytraffic#hasStartLongitude> ?lon1.

?p2 <http://purl.oclc.org/NET/ssnx/ssn#isPropertyOf> ?foi2.
?foi2 <http://www.insight-centre.org/citytraffic#hasStartLatitude> ?lat2.
?foi2 <http://www.insight-centre.org/citytraffic#hasStartLongitude> ?lon2.

{
?obId1 a ?ob.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p1.
?obId1 <http://purl.oclc.org/NET/sao/hasValue> ?v1.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusPollutionData201399>.
}

{
?obId2 a ?ob.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p2.
?obId2 <http://purl.oclc.org/NET/sao/hasValue> ?v2.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusPollutionData184892>.
}}

```

## CityBench Query11
```sql
REGISTER QUERY citybench_query11 AS
SELECT ?obid1 ?ob ?p1 ?v1
 FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusWeatherData0> [range 3s step 1s] 

WHERE { 
{
?obId1 a ?ob.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p1.
?obId1 <http://purl.oclc.org/NET/sao/hasValue> ?v1.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusWeatherData0>.
}

}

```