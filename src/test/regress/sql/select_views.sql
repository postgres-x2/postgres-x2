--
-- SELECT_VIEWS
-- test the views defined in CREATE_VIEWS
--

SELECT * FROM street ORDER BY name,cname,thepath::text;

SELECT name, #thepath FROM iexit ORDER BY 1, 2;

SELECT * FROM toyemp WHERE name = 'sharon';
