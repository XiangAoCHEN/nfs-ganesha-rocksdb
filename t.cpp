#include <cinttypes>
#include <iostream>

class PageAddress {
public:
    // 默认构造函数
    PageAddress() = default;

    // 参数化构造函数
    PageAddress(uint32_t space_id, uint32_t page_id) noexcept 
        : space_id(space_id), page_id(page_id) {}

    // 复制赋值操作符
    PageAddress& operator=(const PageAddress& other) noexcept {
        if (this != &other) {
            space_id = other.space_id;
            page_id = other.page_id;
        }
        return *this;
    }

    // 获取space_id
    [[nodiscard]] uint32_t SpaceId() const noexcept {
        return space_id;
    }

    // 获取page_id
    [[nodiscard]] uint32_t PageId() const noexcept {
        return page_id;
    }

    // 等于操作符
    bool operator==(const PageAddress &other) const noexcept {
        return space_id == other.space_id && page_id == other.page_id;
    }

    // 不等于操作符
    bool operator!=(const PageAddress &other) const noexcept {
        return !(operator==(other));
    }

private:
    uint32_t space_id {};
    uint32_t page_id {};
};

int main() {
    PageAddress addr1(1, 100);
    PageAddress addr2;

    // 使用赋值操作符
    addr2 = addr1;

    if (addr2 == addr1) {
        std::cout << "Addresses are equal" << std::endl;
    } else {
        std::cout << "Addresses are not equal" << std::endl;
    }

    return 0;
}
