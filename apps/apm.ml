
open Util
open Future

type profile = {
  firstname : string;
  lastname : string;
  sex : string;
  age : int;
  lo_agepref : int;
  hi_agepref : int;
  profession : string;
  has_children : bool;
  wants_children : bool;
  leisure : string;
  drinks : bool;
  smokes : bool;
  music : string;
  orientation : string;
  build : string;
  height : string
}

let convert (p : string) : profile =
  let s = String.concat " " (Str.split (Str.regexp_string "@") p) in
  Scanf.sscanf s "%s@ %s@ %s@ %d %d %d %s@ %B %B %s@ %B %B %s@ %s@ %s@ %s"
  (fun firstname lastname sex age lo_agepref hi_agepref profession has_children
       wants_children leisure drinks smokes music orientation build height ->
   { firstname = firstname;
     lastname = lastname;
     sex = sex;
     age = age;
     lo_agepref = lo_agepref;
     hi_agepref = hi_agepref;
     profession = profession;
     has_children = has_children;
     wants_children = wants_children;
     leisure = leisure;
     drinks = drinks;
     smokes = smokes;
     music = music;
     orientation = orientation;
     build = build;
     height = height
   })

let print_profile ({
     firstname = firstname;
     lastname = lastname;
     sex = sex;
     age = age;
     lo_agepref = lo_agepref;
     hi_agepref = hi_agepref;
     profession = profession;
     has_children = has_children;
     wants_children = wants_children;
     leisure = leisure;
     drinks = drinks;
     smokes = smokes;
     music = music;
     orientation = orientation;
     build = build;
     height = height } : profile) : unit =
  Printf.printf "%s %s\n" firstname lastname;
  Printf.printf "  sex: %s  age: %d  profession: %s\n" sex age profession;
  Printf.printf "  %s  %s\n" (if drinks then "social drinker" else "nondrinker") (if smokes then "smoker" else "nonsmoker");
  Printf.printf "  %s  %s\n"
    (if has_children then "has children" else "no children")
    (if wants_children then "wants children" else "does not want children");
  Printf.printf "  prefers a %s partner between the ages of %d and %d\n"
    (if (orientation="straight" && sex="F") || (orientation = "gay/lesbian" && sex="M") then "male" else "female")
    lo_agepref hi_agepref;
  Printf.printf "  likes %s music and %s\n" music leisure


let print_matches (n : string) ((p, ps) : profile * (float * profile) list) : unit =
  print_string "------------------------------\nClient: ";
  print_profile p;
  Printf.printf "\n%s best matches:\n" n;
  List.iter (fun (bci, profile) ->
    Printf.printf "------------------------------\nCompatibility index: %f\n" bci; print_profile profile) ps;
  print_endline ""



(*module PSeq = Sequence.Seq(PFuture)(struct let use_mpi = true end)*)
module PSeq = Sequence.ListSeq
		
(* apm computes the potential love of your life.  The filename of a file
 * containing candidate profiles is in location 0 of the given array.
 * The number of matches desired is in location 1.  The first and last name
 * of the (human) client are in location 2 and 3, respectively.  The client's
 * profile must be in the profiles file.  Results are output to stdout. *)

(*make parrallel...?*)
let read_file filename =
  let f = open_in filename in
  let rec next accum =
    match (try Some (input_line f) with End_of_file -> None) with
    | None -> accum
    | Some line -> next ((convert line) :: accum)
  in
  let profs = next [] in
  close_in f;
  profs
;;

let filter (l: 'a PSeq.t) (f: 'a -> bool): 'a PSeq.t =
  let l_array = PSeq.array_of_seq l in
  let bits = PSeq.map (fun x -> if f x then 1 else 0) l in
  let bits_array = PSeq.array_of_seq bits in
  let length = PSeq.reduce (fun sum x -> sum + x) 0 bits in
  let bitsum = PSeq.scan (fun prev curr -> prev + curr) 0 bits in
  let bitsum_array = PSeq.array_of_seq bitsum in
  let filteredArray = Array.make length (PSeq.nth l 0) in
  PSeq.tabulate (fun i ->
		 if bits_array.(i) == 0 then i
		 else (Array.set filteredArray (bitsum_array.(i)-1) l_array.(i); i))
		(PSeq.length l);
  PSeq.seq_of_array filteredArray
;;

let update (results: (int * float * profile) array)
	   ((score, p): float * profile) (count: int): unit =
  let old_results = PSeq.seq_of_array results in
  let (index, _, _) = PSeq.reduce (fun (curr_i, _, _) (i, s, _) ->
				   if curr_i = -1 then
				     if (score > s) then (i, 0.0, p)
				     else (curr_i, 0.0, p)
				   else (curr_i, 0.0, p)) (-1, 0.0, p) old_results in
  if index != -1 then
    (PSeq.tabulate (fun i -> if i < index then i
			    else if i = index then
			      (Array.set results i (i, score, p); i)
			     else (Array.set results i (PSeq.array_of_seq old_results).(i-1); i))
		   count; ())
  else () (*do nothing*)
  
let matchme (args : string array) : unit =
  let match_count = int_of_string args.(1) in
  let profs = read_file (args.(0)) in
  let prof_seq = PSeq.seq_of_array (Array.of_list profs) in
  let curr_profile = PSeq.nth (filter prof_seq (fun p ->
						Printf.printf "%s%s" p.firstname args.(2);p.firstname = args.(2)
							 && p.lastname = args.(3))) 0 in
  Printf.printf "here";
  let other_profs = filter prof_seq (fun p -> p.firstname != args.(2)
					      || p.lastname != args.(3)) in
  let score_seq = PSeq.map (*!! need to change score*)
		    (fun p -> (1.0, p)) other_profs in
  let results = Array.init match_count (fun i -> (i, 0.0, curr_profile)) in
  PSeq.map (fun r -> update results r match_count) score_seq;
  let processed_results = PSeq.map (fun (i, s, p) -> (s, p)) (PSeq.seq_of_array results) in
  let final_results = Array.to_list (PSeq.array_of_seq
				       (filter processed_results (fun (s, p) -> p != curr_profile))) in
  print_matches args.(1) (curr_profile, final_results)
;;



						    




















