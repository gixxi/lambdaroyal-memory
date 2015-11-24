set term png font "arial,9"
set datafile separator ";"
set style data linespoint
set decimalsign "."
set timefmt "%Y-%m-%d %H:%M:%S"
set xdata time
set output "projection.png"
set yrange [0 : 150]
set format x "%Y-%m-%d %H:%M"
set xtics rotate by -90
set ylabel 'runtime (ms) / number of part-orders * 10^-2'
set title 'Projection using key search over 3 collections, 3 iterations performed concurrently'
set grid
plot 'projection.dat' using 1:2 title 'runtime', \
  'projection.dat' using 1:3 title 'number of part orders'






