#!/bin/bash

# =============================================
# FILEDROP TOKEN (ISI DENGAN TOKEN ANDA)
# =============================================
TOKEN="abcdefghijklmnopqrstuvwxyz123456"   # Ganti dengan token asli Anda
# =============================================

# Tampilkan cara penggunaan jika tidak ada argumen
if [ $# -eq 0 ]; then
    echo "Penggunaan: $0 <pola-wildcard>"
    echo "Contoh: $0 '*.txt'  atau  $0 '*1.txt'"
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
        
        # Gunakan token dengan parameter --token (sesuaikan dengan perintah filedrop Anda)
        filedrop upload "$file" --token "$TOKEN"
        
        if [ $? -eq 0 ]; then
            echo "✅ Berhasil: $file"
        else
            echo "❌ Gagal: $file"
        fi
    fi
done
