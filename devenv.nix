{ pkgs, lib, config, inputs, ... }:

{

  # devenv shell format-python
  scripts.format-python.exec = ''
    black .
  '';

  languages.nix.enable = true;
  languages.python.enable = true; 

  env."PROJECT_NAME"= "OpenStack-DB-Usage-Exporter";

  packages = [ 
    pkgs.git 
    pkgs.mariadb_106
    (pkgs.python3.withPackages (ps: with ps; [ 
      jinja2 
      pymysql 
      black
    ]))
  ];

{
  pre-commit.hooks = {
    # format Python code
    black.enable = true;
    # shellcheck.enable = true;
    # detect-private-keys.enable = true;
    # yamllint.enable = true;
    # nixpkgs-fmt.enable = true;
  };
}

  services.mysql = {
    enable = true;
    package = pkgs.mariadb_106;
    settings = {
      mysqld = {
        group_concat_max_len = 320000;
        log_bin_trust_function_creators = 1;
        sql_mode = "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
      };
    };
  };
}