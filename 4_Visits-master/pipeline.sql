

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.95c2a8be-9f4a-4584-8c3e-1bafd021159b"),
    peope_locations=Input(rid="ri.foundry.main.dataset.a3c4975e-d75d-4ec5-a2c9-97ba251d42a3")
)
SELECT *, count(location_id) as count_loc
FROM peope_locations
group by person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a3c4975e-d75d-4ec5-a2c9-97ba251d42a3"),
    location=Input(rid="ri.foundry.main.dataset.efac41e8-cc64-49bf-9007-d7e22a088318"),
    person=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
SELECT location.*,person.person_id 
FROM location RIGHT JOIN person on location.location_id=person.location_id

