package io.github.parliament.page;

import com.google.common.base.Preconditions;
import lombok.*;

@EqualsAndHashCode
@ToString
public class Page {
    @Getter
    private int no;
    @Getter
    private int location;
    @Getter
    private byte[] content;
    @Getter
    private Page upLevelPage;

    @Builder
    public Page(int no, int location, @NonNull byte[] content) {
        Preconditions.checkArgument(no >= 0);
        Preconditions.checkArgument(location > 0);
        this.no = no;
        this.location = location;
        this.content = content;
    }

    public synchronized void insertBytes(int i, byte[] src) {
        byte[] newContent = new byte[content.length + src.length];
        System.arraycopy(content, 0, newContent, 0, i);
        System.arraycopy(src, 0, newContent, i, src.length);
        System.arraycopy(content, i, newContent, i + src.length, content.length - i);
        this.content = newContent;
    }

    // include i. exclude j
    public synchronized void replaceBytes(int i, int j, byte[] src) {
        Preconditions.checkArgument(i < j);
        Preconditions.checkArgument(j - i == src.length);
        Preconditions.checkArgument(j <= content.length);

        int c = 0;
        while (i < j) {
            content[i] = src[c];
            c++;
            i++;
        }
    }

    // include i. exclude j
    public synchronized void copyBytes(int i, int j, byte[] dst) {
        System.arraycopy(content, i, dst, 0, j - i);
    }

    public synchronized void updateContent(byte[] content) {
        this.content = content;
    }

    public void setUpLevelPage(Page upLevelPage) {
        this.upLevelPage = upLevelPage;
    }
}
