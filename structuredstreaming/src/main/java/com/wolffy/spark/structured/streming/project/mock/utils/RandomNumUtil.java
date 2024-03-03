package com.wolffy.spark.structured.streming.project.mock.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class RandomNumUtil {
    private static final Random RANDOM = new Random();

    /**
     * 返回一个随机的整数[from, to]
     *
     * @param from 起始值
     * @param to 终止值
     * @return 随机生成的整数
     */
    public static int randomInt(int from, int to) {
        if (from > to) {
            throw new IllegalArgumentException(String.format("from = %d 应该小于 to = %d", from, to));
        }
        return RANDOM.nextInt(to - from + 1) + from;
    }

    /**
     * 随机的Long [from, to]
     *
     * @param from 起始值
     * @param to 终止值
     * @return 随机生成的Long值
     */
    public static long randomLong(long from, long to) {
        if (from > to) {
            throw new IllegalArgumentException(String.format("from = %d 应该小于 to = %d", from, to));
        }
        return Math.abs(RANDOM.nextLong()) % (to - from + 1) + from;
    }

    /**
     * 生成一系列的随机值
     *
     * @param from 起始值
     * @param to 终止值
     * @param count 数量
     * @param canRepeat 是否允许随机数重复
     * @return 随机生成的整数列表
     */
    public static List<Integer> randomMultiInt(int from, int to, int count, boolean canRepeat) {
        if (from > to) {
            throw new IllegalArgumentException(String.format("from = %d 应该小于 to = %d", from, to));
        }
        if (canRepeat) {
            List<Integer> list = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                list.add(randomInt(from, to));
            }
            return list;
        } else {
            Set<Integer> set = new HashSet<>();
            while (set.size() < count) {
                set.add(randomInt(from, to));
            }
            return new ArrayList<>(set);
        }
    }

    public static void main(String[] args) {
        // canRepeat 是否可以重复
        System.out.println(randomMultiInt(1, 15, 10, true));
        System.out.println(randomMultiInt(1, 80, 10, false));
    }
}

