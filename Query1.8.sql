SELECT 
  A.primaryName,
  F.primaryTitle,
  COUNT(DISTINCT J.category) AS nbResponsabilites
FROM dbo.tJob AS J
JOIN dbo.tArtist AS A ON A.idArtist = J.idArtist
JOIN dbo.tFilm   AS F ON F.idFilm   = J.idFilm
GROUP BY A.primaryName, F.primaryTitle
HAVING COUNT(DISTINCT J.category) > 1
ORDER BY A.primaryName, F.primaryTitle;