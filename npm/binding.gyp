{
  "targets": [
    {
      "target_name": "amoradb",
      "sources": [ "./src/native.c" ],
      "cflags!": ["-fno-rtti", "-fno-exceptions"],
      "cflags": [
        "-O3",
        "-std=c11"
      ],
      "conditions": [
        ["OS=='win'", {
          "msvs_settings": {
            "VCCLCompilerTool": {
              "Optimization": "Full",
              "FavorSizeOrSpeed": "1"
            }
          }
        }]
      ]
    }
  ]
}
