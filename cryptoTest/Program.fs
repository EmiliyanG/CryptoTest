// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

//#r "System.IO.dll"
//#r "System.Security.dll"
open System
open System.IO
open System.Security.Cryptography


module Regex = 
    open System.Text.RegularExpressions
    open System

    [<Literal>] 
    let pathToFileRegex = """^(?:[\w]\:|\\)(\\[a-zA-Z_\-\s0-9\.\$\(\)]+)+\.(.+)$"""

    let (|ParseRegex|_|) regex str =
        let m = Regex(regex).Match(str)
        if m.Success then Some str
        else None
    
    let (|FilePath|_|) arg = 
        if String.Compare("--filePath", arg, StringComparison.OrdinalIgnoreCase) = 0
        then Some() else None


    let (|ValidPath|_|) arg = 
        match arg with
        | ParseRegex pathToFileRegex s -> Some s
        | _ -> None

    
module ParseCommandLineArgs = 
    open Regex
  
    type CommandLineOptions = {filePath: string option}

    let defaultOptions = {filePath = None}
    
    let rec internal parseCommandLine optionsSoFar args  = 
        match args with 
        // empty list means we're done.
        | [] -> optionsSoFar  

        // match filePath flag
        | FilePath::xs -> 
            match xs with 
            | ValidPath x ::xss -> 
                let newOptionsSoFar = { optionsSoFar with filePath=Some x}
                parseCommandLine newOptionsSoFar xss  
            | _ ->
                eprintfn "--filePath needs a second argument or the provided argument is not valid" 
                parseCommandLine optionsSoFar xs  

        // handle unrecognized option and keep looping
        | x::xs -> 
            eprintfn "Option '%s' is not recognized" x
            parseCommandLine optionsSoFar xs  
    
    let parseCommandLineArguments args =
        parseCommandLine defaultOptions args
    
    let printExceptedUsage() = 
        printfn "\nexpected usage: "
        printfn "--filePath <path-to-file>\<filename.extension>"


module Async =
    open System.Threading
    let inline ParallelWithThrottle (millisecondsTimeout:int) (limit:int) (operations:Async<'b> seq) =
        let semaphore = new System.Threading.SemaphoreSlim(limit, limit)
        let mutable count = (operations |> Seq.length) 
        operations 
        |> Seq.map(fun op -> async {
                let! isHandleAquired = Async.AwaitTask <| semaphore.WaitAsync(millisecondsTimeout=millisecondsTimeout)
                if isHandleAquired then 
                    try
                        return! op
                    finally 
                        if Interlocked.Decrement(&count) = 0 then
                            semaphore.Dispose()
                        else semaphore.Release() |> ignore
                else return! failwith "Failled to aquire handle" })
        |> Async.Parallel 



let getStream(s) = 
    new FileStream(s, FileMode.Open, FileAccess.Read)


let readBytesFromFile filePath offset count =
    async{
        let stream  = getStream(filePath)
        let a = Array.zeroCreate count
        stream.Position <- offset
        stream.Read(a, 0, count) |> ignore
        stream.Dispose()
        stream.Close()
        return offset, MD5Cng.Create().ComputeHash(a)
    }


let rec generateListOfAsyncTasks filePath (length:int64) count offset asyncList = 
    match length with
    | l when l > int64(offset) -> 
        (readBytesFromFile filePath offset count)::asyncList
        |> generateListOfAsyncTasks filePath length count (offset+int64(count))
    |_ -> asyncList



let processFile s = 
    let stream = getStream(s)
    let length = stream.Length
    stream.Dispose()

    let x = 
        generateListOfAsyncTasks s length 1000000 0L []
        |> Async.ParallelWithThrottle 600000 4  //current timeout - 6 minutes
        |> Async.RunSynchronously
        |> Array.sortBy (fun (n,_) -> n)
        |> Array.map (fun (_,s) -> s)
        |> Array.concat
    
    
    printfn "%A" (MD5.Create().ComputeHash(x))

open ParseCommandLineArgs

[<EntryPoint>]
let main argv = 

    let a = 
        argv
        |> Array.toList
        |> parseCommandLineArguments

    match a.filePath with
    | Some p -> 
        let print = 
            printfn "%A > %s" (System.DateTime.Now)

        print ("Started processing file \"" + p + "\"") 
        processFile p
        print "finished"

    | None -> 
        printExceptedUsage()
        
    
    0 // return an integer exit code
