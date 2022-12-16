python -m grpc_tools.protoc -I..\..\..\protos\ --python_out=. --grpc_python_out=. ..\..\..\protos\* 
# Then do a replacement to fix imports
$regex = "^import ([\w\d]+_pb2)"
Get-ChildItem '*.py' | ForEach-Object {
    $c = (Get-Content $_.FullName) -replace $regex, 'from . import $1' -join "`r`n"
    [IO.File]::WriteAllText($_.FullName, $c)
}
