port module Ports.JSSearch exposing (..)

import Json.Decode


port startSearch : () -> Cmd msg


port searchProgressed : (Json.Decode.Value -> msg) -> Sub msg


port matchFound : (String -> msg) -> Sub msg


port searchStepChanged : (Json.Decode.Value -> msg) -> Sub msg



--port searchError:
