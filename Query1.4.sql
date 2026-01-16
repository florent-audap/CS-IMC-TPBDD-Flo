SELECT TOP 1 
    A.birthYear,
    COUNT(DISTINCT A.idArtist) AS nbActors
FROM dbo.tArtist AS A
JOIN dbo.tJob    AS J ON A.idArtist = J.idArtist
WHERE A.birthYear <> 0
  AND J.category = 'acted in'
GROUP BY A.birthYear
ORDER BY nbActors DESC;
