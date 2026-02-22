#!/bin/bash

# Script: upload_mediafire_env.sh
# Mengunggah file ke MediaFire via filedrop dengan token dari environment variable

# Cek apakah environment variable FILEDROP_TOKEN sudah diset
if [ -z "$FILEDROP_TOKEN" ]; then
    echo "❌ Environment variable FILEDROP_TOKEN belum diset."
    echo "   Jalankan: export FILEDROP_TOKEN='token_anda_disini'"
    exit 1
fi

# Tampilkan cara penggunaan jika tidak ada argumen
if [ $# -eq 0 ]; then
    echo "Penggunaan: $0 <pola-wildcard>"
    echo "Contoh: $0 '*.txt'"
    exit 1
fi

pattern="$1"
shopt -s nullglob
files=($pattern)

if [ ${#files[@]} -eq 0 ]; then
    echo "Tidak ada file yang cocok dengan pola: $pattern"
    exit 1
fi

for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo "Mengunggah: $file ..."
        # Gunakan token dari environment variable
        # Sesuaikan parameter dengan tool filedrop Anda
        filedrop upload "$file" --token "$FILEDROP_TOKEN"
        if [ $? -eq 0 ]; then
            echo "✅ Berhasil: $file"
        else
            echo "❌ Gagal: $file"
        fi
    fi
done
