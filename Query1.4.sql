SELECT TOP 1
    birthYear,
    COUNT(*) AS nbActor
FROM dbo.tArtist
WHERE birthYear <> 0
GROUP BY birthYear
ORDER BY nbActor DESC;