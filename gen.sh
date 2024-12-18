#!/bin/bash

protoc "--plugin=$(pwd)/node_modules/ts-proto/protoc-gen-ts_proto" \
    --proto_path=proto/ \
    --ts_proto_opt=onlyTypes=true \
    --ts_proto_opt=useOptionals=messages \
    --ts_proto_opt=useSnakeTypeName=false \
    --ts_proto_opt=esModuleInterop=true \
    --ts_proto_opt=env=browser \
    --ts_proto_opt=comments=false \
    --ts_proto_opt=useOptionals=all \
    --ts_proto_opt=stringEnums=false \
    --ts_proto_opt=useNullAsOptional=false \
    --ts_proto_out=./src/types/pulsar/ \
     ./proto/PulsarApi.proto


# --ts_proto_opt=outputTypeRegistry=true
    # --ts_proto_opt=outputIndex=true \
    # --ts_proto_opt=onlyTypes=true \