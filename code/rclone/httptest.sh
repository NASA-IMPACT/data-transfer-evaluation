token=""

headers='Authorization,'
# rclone -vv lsf --http-no-slash --http-url "https://beta.databridge.gael-systems.com/gss-catalogue/Products" :http: --webdav-vendor other --dump headers --webdav-bearer-token $token --dump bodies

#rclone -vv lsf --http-no-slash custom-source-1: --config=./rclone.conf  --dump  bodies --webdav-vendor other --webdav-bearer-token "$token" --webdav-url https://beta.databridge.gael-systems.com/gss-catalogue/Products --dump headers 


rclone -vv lsjson  --http-no-slash custom-source-1: --config=./rclone.conf  --webdav-vendor other   --http-no-slash --dump responses --webdav-headers 'Referer,https://beta.databridge.gael-systems.com/gss-catalogue/Products?\$format=json'

# rclone info --check-control=false --check-length=true --check-normalization=false --check-streaming=false custom-source-1
