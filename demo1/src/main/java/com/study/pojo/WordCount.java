package com.study.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhang.siwei
 * @time 2022-12-10 16:08
 * @action
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class WordCount {
    private String word;
    private Integer count;
}
