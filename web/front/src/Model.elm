module Model exposing (..)

import Dict exposing (Dict)
import Json.Decode as D


resetSearchResults : SearchResults
resetSearchResults =
    { matches = []
    , progress = NotStarted
    , steps = []
    }


updateProgress : SearchResults -> SearchProgress -> SearchResults
updateProgress results progress =
    { results | progress = InProgress progress }


updateStep : SearchResults -> SearchStep -> SearchResults
updateStep results step =
    if step.step == 6 then
        { results | steps = results.steps ++ [ step ], progress = Finished }

    else
        { results | steps = results.steps ++ [ step ] }


appendMatch : SearchResults -> String -> SearchResults
appendMatch results newMatch =
    { results | matches = results.matches ++ [ newMatch ] }


type alias SearchResults =
    { matches : List String
    , progress : SearchState
    , steps : List SearchStep
    }


type SearchState
    = NotStarted
    | InProgress SearchProgress
    | Finished


type alias SearchDefinition =
    { todo : String }


type alias SearchStep =
    { step : Int
    , description : String
    }


type alias State =
    { definition : Maybe SearchDefinition
    , results : SearchResults
    }


type alias Progress =
    { done : Int
    , total : Int
    , rate : Float
    }


type alias SearchProgress =
    { elapsed : Int
    , eta : String
    , matches : Int
    , overallProgress : Progress
    , perPartitionProgress : Dict String Progress
    }


decodeProgress : D.Decoder Progress
decodeProgress =
    D.map3 Progress
        (D.field "done" D.int)
        (D.field "total" D.int)
        (D.field "rate" D.float)


decodeSearchProgress : D.Decoder SearchProgress
decodeSearchProgress =
    D.map5 SearchProgress
        (D.field "elapsed" D.int)
        (D.field "eta" D.string)
        (D.field "matches" D.int)
        (D.field "overall_progress" decodeProgress)
        (D.field "per_partition_progress" (D.dict decodeProgress))


decodeSearchStep : D.Decoder SearchStep
decodeSearchStep =
    D.map2 SearchStep
        (D.field "step" D.int)
        (D.field "description" D.string)
