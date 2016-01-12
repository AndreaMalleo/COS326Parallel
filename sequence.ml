open System
open Future 
open Mpi


module type S = sig
  type 'a t
  val tabulate : (int -> 'a) -> int -> 'a t
  val seq_of_array : 'a array -> 'a t
  val array_of_seq : 'a t -> 'a array
  val iter: ('a -> unit) -> 'a t -> unit
  val length : 'a t -> int
  val empty : unit  ->'a t
  val cons : 'a -> 'a t -> 'a t
  val singleton : 'a -> 'a t
  val append : 'a t -> 'a t -> 'a t
  val nth : 'a t -> int -> 'a
  val map : ('a -> 'b) -> 'a t -> 'b t
  val map_reduce : ('a -> 'b) -> ('b -> 'b -> 'b) -> 'b -> 'a t -> 'b
  val reduce : ('a -> 'a -> 'a) -> 'a -> 'a t -> 'a
  val flatten : 'a t t -> 'a t
  val repeat : 'a -> int -> 'a t
  val zip : ('a t * 'b t) -> ('a * 'b) t
  val split : 'a t -> int -> 'a t * 'a t
  val scan: ('a -> 'a -> 'a) -> 'a -> 'a t -> 'a t
end



(*******************************************************)
(* Sequential Sequences Based on a List Representation *)
(*******************************************************)

module ListSeq : S = struct

  type 'a t = 'a list

  let length = List.length

  let empty () = []

  let cons (x:'a) (s:'a t) = x::s

  let singleton x = [x]

  let append = List.append

  let tabulate f n =
    let rec helper acc x =
      if x = n then List.rev acc
      else helper ((f x)::acc) (x+1) in
    helper [] 0

  let nth = List.nth

  let filter = List.filter

  let map = List.map

  let reduce = List.fold_left

  let map_reduce m r b s = reduce r b (map m s)

  let repeat x n =
    let rec helper x n acc =
      if n = 0 then acc else helper x (n-1) (x::acc) in
    helper x n []

  let flatten = List.flatten

  let zip (s1,s2) = List.combine s1 s2

  let split s i =
    let rec helper s i acc =
      match s,i with
        | [],_ -> failwith "split"
        | _,0 -> (List.rev acc,s)
        | h::t,_ -> helper t (i-1) (h::acc) in
    helper s i []

  let iter = List.iter

  let array_of_seq = Array.of_list

  let seq_of_array = Array.to_list

  let scan f b s = 
    let (_,xs) = List.fold_left (fun (v,ls) e -> let r = f v e in (r,r::ls)) (b,[]) s in
    List.rev xs

end


(*******************************************************)
(* Parallel Sequences                                  *)
(*******************************************************)

module type SEQ_ARGS = sig 
  val use_mpi: bool
end

module Seq (Par : Future.S) (Arg : SEQ_ARGS) : S = struct

  type 'a t = 'a array

  let num_cores = System.cpu_count ()

  let num_chunks = num_cores * 1
						     
  let force_arrays (n: int) (a: 'a array Par.future array) (num_chunks: int) : 'a array = 
    let forced = Array.map (fun x -> Par.force x) a in
    let result = Array.make n None in
    let index = ref 0 in
    for i = 0 to (num_chunks-1) do
      let lo = (n * i) / num_chunks in
      let hi = (n * (i+1) / num_chunks) in
      for j = 0 to (hi-lo-1) do
        Array.set result (!index) (Some (forced.(i)).(j));
	index := !index + 1
      done
    done;
    Array.map (fun x -> match x with
			| Some r -> r
			| None -> failwith "should not reach") result
  ;;
		       				  
  let tabulate f n =
    let num_chunks = if n < num_chunks then n
		     else num_chunks in
    let result = Array.init num_chunks (fun i ->
      let lo = (n * i) / num_chunks in
      let hi = (n * (i+1) / num_chunks) in
      Par.future (Array.init (hi - lo))
		 (fun i -> f (i + lo))) in
    force_arrays n result num_chunks 
  ;;
    
  let seq_of_array a = Array.copy a

  let array_of_seq seq = Array.copy seq

  let iter f seq = 
    let copyseq = Array.copy seq in
    Array.iter f copyseq

  let length seq = Array.length seq
				
  let empty () = [||]

  let cons elem seq =
    Array.init (length seq + 1) (fun i -> 
    if (i = 0) then elem
    else seq.(i-1))

  let singleton elem = Array.of_list [elem] 

  let append seq1 seq2 =
    let copyseq1 = Array.copy seq1 in
    let copyseq2 = Array.copy seq2 in
    Array.append copyseq1 copyseq2

  let nth seq i = seq.(i)

  let map f seq =
    tabulate (fun i -> f seq.(i)) (length seq)
  ;;

  let map_reduce m r b seq =
    let num_chunks = if (Array.length seq) < num_chunks
		     then Array.length seq
		     else num_chunks in
    let copyseq = Array.copy seq in
    let n = length seq in  
    let result = Array.init num_chunks (fun i ->
     let lo = (n * i) / num_chunks in
     let hi = (n * (i+1) / num_chunks) in
     if lo = (hi-1) then Par.future (fun _ -> m copyseq.(lo)) ()
     else Par.future (fun x ->
	    let maped_array = Array.map m x in
	    Array.fold_left r (Array.get maped_array 0) (Array.sub maped_array 1 (hi-lo-1)))
		     (Array.sub copyseq lo (hi-lo))) in  
    Array.fold_left (fun acc elt -> r acc (Par.force elt)) b result
 ;;
   
  (*order is acc elt*)
 let reduce f b seq =
   let num_chunks = if (Array.length seq) < num_chunks
		    then Array.length seq
		    else num_chunks in
   let copyseq = Array.copy seq in
   let n = length seq in
   let result = Array.init num_chunks (fun i ->
     let l = (n * i) / num_chunks in
     let r = (n * (i+1) / num_chunks) in
     if l = (r-1) then Par.future (fun _ -> copyseq.(l)) ()
     else
       Par.future (Array.fold_left f (Array.get copyseq l))
		  (Array.sub copyseq (l+1) (r-l-1))) in
   Array.fold_left (fun acc elt -> f acc (Par.force elt)) b result
  ;;

  let flatten seqseq =
    let copyseqseq = Array.copy seqseq in
    Array.fold_left (fun acc b -> Array.append acc b) (empty ()) (array_of_seq copyseqseq)
      
  let repeat elem num = Array.make num elem


  (*check for lengths*)
  let zip (seq1,seq2) =
    let l1 = length seq1 in
    let l2 = length seq2 in
    if (l1 < l2) then  
      Array.init l1 (fun i-> (seq1.(i),seq2.(i))) 
    else 
     Array.init l2 (fun i-> (seq1.(i), seq2.(i))) 
    

  let split seq x = 
    if x > length seq then
      failwith "index beyond the limit of the sequence"
   else 
     let copyseq = Array.copy seq in
     let fst = Array.sub copyseq 0 x in
     let snd = Array.sub copyseq x ((length copyseq)-x) in
      (fst,snd)


  (*******************************************************)
  (* Parallel Prefix Sum                                 *)
  (*******************************************************)

  (* Here you will implement a version of the parallel prefix scan for a sequence 
   * [a0; a1; ...], the result of scan will be [f base a0; f (f base a0) a1; ...] *)
  type 'a message =
    | Up of 'a
    | Down of 'a option
    | Result of 'a t

  let make_chunk seq lo hi =
    tabulate (fun i -> seq.(lo+i)) (hi-lo)
  ;;

  let rec child (ch: ('a message, 'a message) Mpi.channel)
	    (chunk, chunk_size, base, f)  =
    (*up calculation*)
    if (length chunk) <= chunk_size then
      let result = match base with
      | Some b -> Array.fold_left f b chunk
      | None ->
	 if (length chunk) = 1 then chunk.(0)
	 else Array.fold_left f chunk.(0)
				(Array.sub chunk 1 ((length chunk)-1)) in
      Mpi.send ch (Up result);

      (*down calculation*)
      let from_left = match Mpi.receive ch with
	| Down(x) -> x
	| _ -> failwith "should not reach" in
      let scan_result = Array.init (length chunk) (fun i ->
        let curr_elt =
	  if i = 0 then
	    match from_left with
	    | Some x -> f x chunk.(i)
	    | None ->
	       match base with
	       | Some b -> f b chunk.(i)
	       | None -> failwith "should not reach"
	  else f chunk.(i-1) chunk.(i) in
	chunk.(i) <- curr_elt; curr_elt) in
      Mpi.send ch (Result scan_result)
      
    else
      (*make childern*)
      let (l_chunk, r_chunk) = split chunk ((length chunk)/2) in
      let left_child = Mpi.spawn child (l_chunk, chunk_size, base, f) in
      let right_child = Mpi.spawn child (r_chunk, chunk_size, None, f) in

      (*up calculation*)
      let l_result =
	match Mpi.receive left_child with
	| Up(r) -> r
	| _ -> failwith "should not reach" in
      let r_result =
	match Mpi.receive right_child with
	| Up(r) -> r
	| _ -> failwith "should not reach" in
      let result = f l_result r_result in
      Mpi.send ch (Up result);

      (*down calculation*)
      let from_left =
	match Mpi.receive ch with
	| Down(x) -> x
	| _ -> failwith "should not reach" in
      Mpi.send left_child (Down from_left);
      (match from_left with
       | Some x -> Mpi.send right_child (Down (Some (f x l_result)))
       | None -> Mpi.send right_child (Down (Some l_result)));
      
      (*send scan result*)
      let l_scan =
	match Mpi.receive left_child with
	| Result(r) -> r
	| _ -> failwith "should not reach" in
      let r_scan =
	match Mpi.receive right_child with
	| Result(r) -> r
	| _ -> failwith "should not reach" in
      let scan_result = Array.append l_scan r_scan in
      Mpi.send ch (Result scan_result);

      (*clean up*)
      Mpi.wait_die left_child;
      Mpi.wait_die right_child
	
  let scan (f: 'a -> 'a -> 'a) (base: 'a) (seq: 'a t) : 'a t =
    let num_chunks = if (Array.length seq) < num_chunks
		     then Array.length seq
		     else num_chunks in
    let copyseq = Array.copy seq in
    let chunk_size = if num_chunks = 0 then 0
		     else (length copyseq) / num_chunks in
    let ch = Mpi.spawn child (copyseq, chunk_size, Some base, f) in
    let _ = match Mpi.receive ch with
      | Up(r) -> r
      | _ -> failwith "should not reach" in
    Mpi.send ch (Down None);
    let scan_result = match Mpi.receive ch with
      | Result(r) -> r
      | _ -> failwith "should not reach" in
    Mpi.wait_die ch; scan_result
  end





