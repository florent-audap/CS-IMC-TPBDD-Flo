SELECT primaryName, COUNT(*) as counted FROM dbo.tArtist AS A JOIN tJob AS J ON A.idArtist=J.idArtist 
WHERE J.category='acted in' GROUP BY A.primaryName HAVING COUNT(*)>1