pub(crate) const TINY_CLUSTER: &str = r#"{
  "fab2spine": [
    {
      "a": 4,
      "b": 0,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 4,
      "b": 1,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 5,
      "b": 2,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 5,
      "b": 3,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 12,
      "b": 0,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 12,
      "b": 1,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 13,
      "b": 2,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 13,
      "b": 3,
      "bandwidth": 40000000000,
      "delay": 1000
    }
  ],
  "planes": [
    [
      {
        "id": 0,
        "kind": "Switch"
      },
      {
        "id": 1,
        "kind": "Switch"
      }
    ],
    [
      {
        "id": 2,
        "kind": "Switch"
      },
      {
        "id": 3,
        "kind": "Switch"
      }
    ]
  ],
  "pods": [
    {
      "fabs": [
        {
          "id": 4,
          "kind": "Switch"
        },
        {
          "id": 5,
          "kind": "Switch"
        }
      ],
      "racks": [
        {
          "host2tor": [
            {
              "a": 6,
              "b": 8,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 7,
              "b": 8,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ],
          "hosts": [
            {
              "id": 6,
              "kind": "Host"
            },
            {
              "id": 7,
              "kind": "Host"
            }
          ],
          "tor": {
            "id": 8,
            "kind": "Switch"
          }
        },
        {
          "host2tor": [
            {
              "a": 9,
              "b": 11,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 10,
              "b": 11,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ],
          "hosts": [
            {
              "id": 9,
              "kind": "Host"
            },
            {
              "id": 10,
              "kind": "Host"
            }
          ],
          "tor": {
            "id": 11,
            "kind": "Switch"
          }
        }
      ],
      "tor2fab": [
        {
          "a": 8,
          "b": 4,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 8,
          "b": 5,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 11,
          "b": 4,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 11,
          "b": 5,
          "bandwidth": 40000000000,
          "delay": 1000
        }
      ]
    },
    {
      "fabs": [
        {
          "id": 12,
          "kind": "Switch"
        },
        {
          "id": 13,
          "kind": "Switch"
        }
      ],
      "racks": [
        {
          "host2tor": [
            {
              "a": 14,
              "b": 16,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 15,
              "b": 16,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ],
          "hosts": [
            {
              "id": 14,
              "kind": "Host"
            },
            {
              "id": 15,
              "kind": "Host"
            }
          ],
          "tor": {
            "id": 16,
            "kind": "Switch"
          }
        },
        {
          "host2tor": [
            {
              "a": 17,
              "b": 19,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 18,
              "b": 19,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ],
          "hosts": [
            {
              "id": 17,
              "kind": "Host"
            },
            {
              "id": 18,
              "kind": "Host"
            }
          ],
          "tor": {
            "id": 19,
            "kind": "Switch"
          }
        }
      ],
      "tor2fab": [
        {
          "a": 16,
          "b": 12,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 16,
          "b": 13,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 19,
          "b": 12,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 19,
          "b": 13,
          "bandwidth": 40000000000,
          "delay": 1000
        }
      ]
    }
  ]
}
"#;

pub(crate) const TINY_CLUSTER_OVERSUB: &str = r#"{
  "fab2spine": [
    {
      "a": 2,
      "b": 0,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 3,
      "b": 1,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 10,
      "b": 0,
      "bandwidth": 40000000000,
      "delay": 1000
    },
    {
      "a": 11,
      "b": 1,
      "bandwidth": 40000000000,
      "delay": 1000
    }
  ],
  "planes": [
    [
      {
        "id": 0,
        "kind": "Switch"
      }
    ],
    [
      {
        "id": 1,
        "kind": "Switch"
      }
    ]
  ],
  "pods": [
    {
      "fabs": [
        {
          "id": 2,
          "kind": "Switch"
        },
        {
          "id": 3,
          "kind": "Switch"
        }
      ],
      "racks": [
        {
          "host2tor": [
            {
              "a": 4,
              "b": 6,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 5,
              "b": 6,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ],
          "hosts": [
            {
              "id": 4,
              "kind": "Host"
            },
            {
              "id": 5,
              "kind": "Host"
            }
          ],
          "tor": {
            "id": 6,
            "kind": "Switch"
          }
        },
        {
          "host2tor": [
            {
              "a": 7,
              "b": 9,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 8,
              "b": 9,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ],
          "hosts": [
            {
              "id": 7,
              "kind": "Host"
            },
            {
              "id": 8,
              "kind": "Host"
            }
          ],
          "tor": {
            "id": 9,
            "kind": "Switch"
          }
        }
      ],
      "tor2fab": [
        {
          "a": 6,
          "b": 2,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 6,
          "b": 3,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 9,
          "b": 2,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 9,
          "b": 3,
          "bandwidth": 40000000000,
          "delay": 1000
        }
      ]
    },
    {
      "fabs": [
        {
          "id": 10,
          "kind": "Switch"
        },
        {
          "id": 11,
          "kind": "Switch"
        }
      ],
      "racks": [
        {
          "host2tor": [
            {
              "a": 12,
              "b": 14,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 13,
              "b": 14,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ],
          "hosts": [
            {
              "id": 12,
              "kind": "Host"
            },
            {
              "id": 13,
              "kind": "Host"
            }
          ],
          "tor": {
            "id": 14,
            "kind": "Switch"
          }
        },
        {
          "host2tor": [
            {
              "a": 15,
              "b": 17,
              "bandwidth": 10000000000,
              "delay": 1000
            },
            {
              "a": 16,
              "b": 17,
              "bandwidth": 10000000000,
              "delay": 1000
            }
          ],
          "hosts": [
            {
              "id": 15,
              "kind": "Host"
            },
            {
              "id": 16,
              "kind": "Host"
            }
          ],
          "tor": {
            "id": 17,
            "kind": "Switch"
          }
        }
      ],
      "tor2fab": [
        {
          "a": 14,
          "b": 10,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 14,
          "b": 11,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 17,
          "b": 10,
          "bandwidth": 40000000000,
          "delay": 1000
        },
        {
          "a": 17,
          "b": 11,
          "bandwidth": 40000000000,
          "delay": 1000
        }
      ]
    }
  ]
}

"#;
