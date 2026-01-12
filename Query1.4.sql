SELECT TOP 1
    birthYear,
    COUNT(*) AS counted
FROM dbo.tArtist
WHERE birthYear <> 0
GROUP BY birthYear
ORDER BY counted DESC;