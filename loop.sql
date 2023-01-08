SELECT * FROM place;
DO $$
DECLARE
   city CHAR(50);
   country CHAR(50);
	id INT;
BEGIN
	id := '0';
   city = 'city';
   country = 'country';
   FOR counter IN 1..5
      LOOP
		   INSERT INTO place(place_id, place_city, place_country) 
		   VALUES (id + counter, 
            city || counter,
            country || counter);
      END LOOP;
END;
$$