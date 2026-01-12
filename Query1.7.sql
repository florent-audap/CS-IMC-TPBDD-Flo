--SELECT primaryTitle FROM dto.tFilm AS F JOIN (SELECT idFilm, max FROM dto.tJob)
--WHERE J.category='acted in' GROUP BY F.primaryTitle 

SELECT primaryTitle, COUNT(*) AS nbActor
FROM tJob AS J JOIN tFilm AS F ON F.idFilm=J.idFilm
WHERE category = 'acted in'
GROUP BY primaryTitle
HAVING COUNT(*) = (
    SELECT TOP 1 COUNT(*)
    FROM tJob
    WHERE category = 'acted in'
    GROUP BY idFilm
    ORDER BY COUNT(*) DESC
);