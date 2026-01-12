SELECT A.primaryName, COUNT(*) FROM
(select distinct idArtist, category from dbo.tJob) as distCat 
JOIN tArtist as A on A.idArtist=distCat.idArtist GROUP BY A.primaryName HAVING Count(*)>1