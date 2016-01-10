open Array
open Future
open Sequence
open Printf

module S = Seq(PFuture)(struct let use_mpi = true end)

type group = {pop:int; lat:float; lon:float}
type area = {left:float; right:float; top:float; bottom:float}


(* convert latitude to compensate for rectangular projection of map *)
let mercator_conv (lat: float) : float = 
  let pi = (4. *. atan 1.0) in
  let latpi = lat *. (pi /. 180.0) in 
  log ( (tan latpi) +. (1.0 /. (cos latpi)) )


(* used to build parsed data *)
let group_of_data (pop, lat, lon) : group = 
  {pop=pop; lat=(mercator_conv lat); lon=(lon)}


(* compute the row and column in the GUI given the latitude and longitude *)
let rowcol_of_latlon (area: area) (rows,cols) (lat,lon) : int*int =
  let delx = (area.right -. area.left) /. (float_of_int cols) in 
  let dely = (area.top -. area.bottom) /. (float_of_int rows) in 
  let c = int_of_float ((lon -. area.left) /. delx) in
  let r = int_of_float ((lat -. area.bottom) /. dely) in
  let r = if r = rows then r else r + 1 in 
  let c = if c = cols then c else c + 1 in 
  (r,c)


(* latitude and longitude area for a box in terms of rows and colums in the GUI
 * rows, cols:  total number of rows and columns
 * l,b,r,t:     left, bottom, right, and top of box respectively *)
let latlon_of_rowcol (area: area) (rows,cols) (l,b,r,t) : area =
  let delx = (area.right -. area.left) /. (float_of_int cols) in 
  let dely = (area.top -. area.bottom) /. (float_of_int rows) in 
  {left = area.left +. (float_of_int (l - 1)) *. delx;
   right = area.left +. (float_of_int r) *. delx;
   top = area.bottom +. (float_of_int t) *. dely;
   bottom = area.bottom +. (float_of_int (b - 1)) *. dely }


(**************************************************************************
 * Part 1
 * 
 * Find the smallest area encompassing all of the census groups.
 * i.e., the area containing all of the population of the United States
 **************************************************************************)

let closest_to_edge (r1: area) (r2: area) : area =
  {left = (min r1.left r2.left); 
   right = (max r1.right r2.right);
   top = (max r1.top r2.top);
   bottom = (min r1.bottom r2.bottom)}


let encompassing_area (groups: group S.t) : area = 
    let inf = Pervasives.infinity in 
    let ninf = Pervasives.neg_infinity in 
    S.map_reduce 
      (fun g -> {left=g.lon; right=g.lon; top=g.lat; bottom=g.lat}) 
      closest_to_edge 
      {left=inf; right=ninf; top=ninf; bottom=inf} 
      groups


(**************************************************************************
 * Part 2
 *
 * Compute the population within an area by looking at all n census groups
 * and summing their total populations using reduce
 **************************************************************************)

let contains_group (r: area) (g: group) : bool = 
  (g.lon >= r.left && g.lon < r.right) &&
  (g.lat <= r.top && g.lat > r.bottom)


let contains_area (r1: area) (r2: area) : bool = 
  (r2.left >= r1.left && r2.left < r1.right) &&
  (r2.right <= r1.right && r2.right > r1.left) &&
  (r2.top <= r1.top && r2.top > r1.bottom) &&
  (r2.bottom >= r1.bottom && r2.bottom < r1.top)


let population_search (groups: group S.t) (query: area) : int = 
  let g = 
    S.reduce (fun g1 g2 -> 
          match (contains_group query g1, contains_group query g2) with 
          | (false, false) -> {pop=0; lat=query.top; lon=query.left}
          | (true, false) -> g1 
          | (false, true) -> g2 
          | (true, true) -> {pop=g1.pop+g2.pop; lat=g1.lat; lon=g2.lon} 
             ) {pop=0; lat=query.top; lon=query.left} groups in 
  g.pop


(************************************************************************** 
 * Part 3
 *
 * Compute the population within an area by precomputing the total population
 * at each rectangle (x,y) for all groups in rectangles from (1,1) to (x,y).
 * You MUST perform this precompution using parallel prefix scans
 **************************************************************************)

let filterpop (l:'a S.t ) : 'a S.t  =
    let f =  (fun bool i ->
          let (p,c,b) = i in
	  if (b != bool) then 1 else 0)

   (*create bit vector for filter*)
    let bits = S.scan f false l in 
    let bitsum = S.scan (fun a b -> a + b) 0 bits in
    let bitarray = array_of_seq bits in 
    let bitsumarray = array_of_seq bitsum in
    let zipbits = S.zip bits bitsum in
    let origseqarray = seq_to_array sumcoords in
    let length = S.reduce (fun ac n -> ac + n) 0 origseqarray in
    let outlength = S.nth bitsum length in 
    let output = Array.make outlength (S.nth sumcoords 1) in 
    let coordsequence = S.tabulate (fun i ->
        if ((Array.get bitarray i) == 1) then
	  Array.set ((Array.get i bitsumarray)-1) output) length in 
    coordsequence

(*creates a *)
let sumcoordscan ( l: 'a S.t) : 'b S.t =  S.scan (fun g1 g2 -> 
                 let (p1,(x1,y1),boo) = g1 in
		 let (p2,(x2,y2)) = g2 in 
		 if ((x1 == x2) && (y1 == y2))
		    then [(p1+p2, (x2,y2), None]
		   else [(p2, (x2,y2)), Some (p1, (x1,y1))]
                 ) [(0,(0,0)), None]) l 

let precompute (groups: group S.t) (us_area: area) (rows,cols) : int S.t S.t = 
    (*creates a sequence of all groups with (population,coordinates) *)
    let coord g = rowcol_of_latlon us_area  (rows,cols) (g.lat,g.lon) in 
 let popS  = S.map (fun g -> (g.pop,coord g)) groups  in

    (*scan sum sequence elements of same coordinate*)
    let sumcoords = sumcoordscan popS in 
   
    (*one population per coordinate*)
    let coordseq = filter sumcoords in

    let popcoordarray = Array.make cols (0,(0,0)) in
    
    let makepopcoordSS : int S.t S.t = S.scan (fun c box -> 
                let (pop,(x,y)) = box in
		if (x > rows) then let
		  () = Array.set popcoordarray y (S.append c box) in
		 S.empty
		else (S.append c box)) S.empty popcoordarray in 

   let popcoordSeq = seq_of_array popcoordarray in 
   (*extract population*)
   let popSS = S.map (fun (pop,c) -> pop) popcoordSeq in 

   let addcolumns  = S.map (
         fun ups ->
	  S.scan (fun pb p -> p + pb) 0 ups)
              popSS in
   let addtwoS s1 s2 = let zipped = S.zip s1 s2 in
     S.reduce (fun b (fs,se) -> fs + se) S.empty zipped in 
   let addrows = S.scan addtwoS (S.repeat rows 0) addcolumns in
   let subTwoS s1 s2 = let zipped =
			 let chop2 = split s2 1 in
			 S.zip s1 chop2 in
		       S.reduce (fun b (fs,ss) -> ss-fs) S.empty zipped in
   let subleftco = S.scan subTwoS (S.repeat 0 rows) addrows in
   subleftco 
   
let population_lookup (summed_areas: int S.t S.t) (l,b,r,t) : int = 
     let toprightcorner = S.nth (S.nth summed_areas r) t in
     let topleftcorner = S.nth (S.nth summed_areas l) t in 
     let bottomrightcorner = S.nth (S.nth summed_areas r) b in 
     let bottomleftcorner = S.nth (S.nth summer_areas l) b in
     toprightcorner-topleftcorner-bottomrightcorner+bottomleftcorner






