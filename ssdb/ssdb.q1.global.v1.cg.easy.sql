SELECT SUM(CG1.v1),COUNT(*) FROM cycle
WHERE x BETWEEN 0 and 3750
AND   y BETWEEN 0 and 3750;
